import socket
import threading
import time
import random
import logging
import queue

from metrics import MetricsCollector

logger = logging.getLogger(__name__)


class ServerInfo:
	def __init__(self, host, port, capacity):
		self.host = host
		self.port = port
		self.capacity = capacity
		self.active = 0        # Number of active requests currently being processed
		self.last_seen = time.time()


class LoadBalancer:
	def __init__(self, algorithm=1):
		self.lb_host = "127.0.0.1"
		self.server_port = 10000
		self.client_port = 20000
		self.servers = {}       # (host, port) -> ServerInfo
		self.lock = threading.RLock()   # RLock so dispatcher can re-enter while holding _slot_cond
		self.rr_index = 0       # For Round Robin cycling
		self.shutdown_event = threading.Event()
		self.algorithm = algorithm  # 1=RR, 2=LeastConn, 3=Random, 4=WeightedRR

		# Per-server request totals used by Weighted Round Robin (reset on server join)
		self.request_totals = {}

		self.metrics = MetricsCollector()
		self.metrics_file = "lb_metrics.json"

		# Waiting queue: holds requests when all servers are at capacity.
		# Requests are retried until a server frees up or queue_timeout elapses.
		self.queue_max_size = 50       # max pending requests before hard-dropping
		self.queue_timeout = 30         # seconds a request may wait before being dropped
		self.request_queue = queue.Queue(maxsize=self.queue_max_size)
		self._queued_count = 0          # total requests ever enqueued
		self._queue_timeout_count = 0   # total requests dropped due to queue timeout

		# Condition variable: notified by release_server() so retry threads wake up
		# immediately when a slot frees instead of busy-waiting every 50 ms.
		# This also gives queued requests priority over brand-new incoming requests.
		self._slot_cond = threading.Condition(self.lock)  # shares RLock — check+wait is atomic

	def start(self):
		threading.Thread(target=self.listen_for_servers, daemon=True).start()
		threading.Thread(target=self.health_check_loop, daemon=True).start()
		threading.Thread(target=self._metrics_writer_loop, daemon=True).start()
		threading.Thread(target=self._queue_dispatcher_loop, daemon=True).start()
		self.listen_for_clients()

	# ------------------------------------------------------------------ #
	#  Health checking                                                     #
	# ------------------------------------------------------------------ #

	def health_check_loop(self, interval=2):
		while not self.shutdown_event.is_set():
			time.sleep(interval)
			to_remove = []
			with self.lock:
				for (host, port), server in list(self.servers.items()):
					if not self.ping_server(server):
						logger.warning(f"Health check failed: removing server {host}:{port}")
						to_remove.append((host, port))
			with self.lock:
				for key in to_remove:
					self.servers.pop(key, None)
					self.request_totals.pop(key, None)
				# Snapshot utilization after removals
				servers_snap = dict(self.servers)
			self.metrics.snapshot_utilization(servers_snap)

	def ping_server(self, server, timeout=1):
		"""
		Send a PING message and expect PONG. This avoids opening a bare TCP
		connection that the server would mistake for a real client request and
		incorrectly count against its active_requests capacity.
		"""
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.settimeout(timeout)
				sock.connect((server.host, server.port))
				sock.sendall(b"PING")
				return sock.recv(4096) == b"PONG"
		except Exception:
			return False

	# ------------------------------------------------------------------ #
	#  Server registration                                                 #
	# ------------------------------------------------------------------ #

	def listen_for_servers(self):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.bind((self.lb_host, self.server_port))
			sock.listen()
			logger.info(f"Load Balancer listening for servers on {self.lb_host}:{self.server_port}")
			while not self.shutdown_event.is_set():
				try:
					sock.settimeout(1.0)
					conn, _addr = sock.accept()
					threading.Thread(target=self.handle_server_message, args=(conn,), daemon=True).start()
				except socket.timeout:
					continue

	def handle_server_message(self, conn):
		try:
			msg = conn.recv(4096).decode()
			parts = msg.strip().split()
			if not parts:
				return
			if parts[0] == "JOIN" and len(parts) == 4:
				host, port, capacity = parts[1], int(parts[2]), int(parts[3])
				with self.lock:
					self.servers[(host, port)] = ServerInfo(host, port, capacity)
					# Reset WRR counter so the new server ramps up proportionally
					self.request_totals[(host, port)] = 0
				logger.info(f"Server joined: {host}:{port} (capacity {capacity})")
			elif parts[0] == "LEAVE" and len(parts) == 3:
				host, port = parts[1], int(parts[2])
				with self.lock:
					self.servers.pop((host, port), None)
					self.request_totals.pop((host, port), None)
				logger.info(f"Server left: {host}:{port}")
		except Exception as e:
			logger.error(f"Error handling server message: {e}")
		finally:
			conn.close()

	# ------------------------------------------------------------------ #
	#  Client request handling                                            #
	# ------------------------------------------------------------------ #

	def listen_for_clients(self):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.bind((self.lb_host, self.client_port))
			sock.listen()
			logger.info(f"Load Balancer listening for clients on {self.lb_host}:{self.client_port}")
			while not self.shutdown_event.is_set():
				try:
					sock.settimeout(1.0)
					conn, addr = sock.accept()
					threading.Thread(target=self.handle_client_request, args=(conn, addr), daemon=True).start()
				except socket.timeout:
					continue

	def select_server(self):
		"""
		Select a server based on the configured algorithm.
		  1 = Round Robin
		  2 = Least Connections
		  3 = Random
		  4 = Weighted Round Robin (capacity-proportional distribution)
		Returns a ServerInfo object, or None if no server is available.
		"""
		with self.lock:
			available = [(k, s) for k, s in self.servers.items() if s.active < s.capacity]
			if not available:
				return None

			if self.algorithm == 2:
				# Least Connections: fewest active requests
				key, server = min(available, key=lambda item: item[1].active)
			elif self.algorithm == 3:
				# Random
				key, server = random.choice(available)
			elif self.algorithm == 4:
				# Weighted Round Robin: pick the server whose
				# (requests_assigned / capacity) ratio is smallest.
				# This ensures proportional distribution over time.
				key, server = min(
					available,
					key=lambda item: self.request_totals.get(item[0], 0) / item[1].capacity
				)
			else:
				# Round Robin (default, algorithm=1 or unknown)
				idx = self.rr_index % len(available)
				self.rr_index += 1
				key, server = available[idx]

			server.active += 1
			server.last_seen = time.time()
			self.request_totals[key] = self.request_totals.get(key, 0) + 1

		# Record outside the lock to avoid nested lock ordering issues
		self.metrics.record_request(key)
		return server

	def release_server(self, server):
		with self._slot_cond:          # acquires self.lock
			server.active = max(0, server.active - 1)
			self._slot_cond.notify_all()   # notify while holding lock — no missed signals

	def _forward_to_server(self, conn, addr, data, server):
		"""Forward data to server and relay response back to conn. Does NOT close conn."""
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sock:
			try:
				s_sock.connect((server.host, server.port))
				s_sock.sendall(data)
				response = s_sock.recv(4096)
				conn.sendall(response)
				self.metrics.record_completion()
				logger.info(f"Forwarded request from {addr} to {server.host}:{server.port}")
			except Exception as e:
				conn.sendall(b"SERVER FAILURE")
				logger.error(f"Server failure: {server.host}:{server.port} - {e}")
				with self.lock:
					self.servers.pop((server.host, server.port), None)
					self.request_totals.pop((server.host, server.port), None)
			finally:
				self.release_server(server)

	def handle_client_request(self, conn, addr):
		"""
		Read the request and place it into the queue.  The dispatcher is the
		ONLY code path that calls select_server(), which eliminates the race
		between queued requests and new arrivals.
		"""
		try:
			data = conn.recv(4096)
		except Exception as e:
			logger.error(f"Error reading from client {addr}: {e}")
			conn.close()
			return

		try:
			self.request_queue.put_nowait((conn, addr, data, time.time()))
			with self.lock:
				self._queued_count += 1
			logger.info(f"Queued request from {addr} (queue size: {self.request_queue.qsize()})")
		except queue.Full:
			try:
				conn.sendall(b"NO AVAILABLE SERVER")
			except Exception:
				pass
			conn.close()
			logger.warning(f"Queue full, dropping request from {addr}")

	def _queue_dispatcher_loop(self):
		"""
		Single dispatcher thread — the sole caller of select_server().

		Because no other code path calls select_server(), queued requests
		are never starved by new incoming requests.  I/O is offloaded to
		per-request threads so the dispatcher is never blocked on network.
		"""
		while not self.shutdown_event.is_set():
			try:
				conn, addr, data, enqueue_time = self.request_queue.get(timeout=0.1)
			except queue.Empty:
				continue

			# Wait until a slot is free or the request times out
			while True:
				elapsed = time.time() - enqueue_time
				if elapsed >= self.queue_timeout:
					try:
						conn.sendall(b"REQUEST TIMEOUT")
					except Exception:
						pass
					conn.close()
					with self.lock:
						self._queue_timeout_count += 1
					logger.warning(f"Request from {addr} timed out after {elapsed:.1f}s in queue")
					break

				# Acquire the shared lock, check for a slot, and wait — all atomically.
				# Because release_server() also notifies under the same lock, signals
				# cannot be missed between the check and the wait.
				with self._slot_cond:
					server = self.select_server()
					if server:
						threading.Thread(
							target=self._dispatch_and_close,
							args=(conn, addr, data, server),
							daemon=True
						).start()
						break
					remaining = self.queue_timeout - elapsed
					self._slot_cond.wait(timeout=min(1.0, remaining))

	def _dispatch_and_close(self, conn, addr, data, server):
		"""Forward one request to a backend server and close the client connection."""
		try:
			self._forward_to_server(conn, addr, data, server)
		finally:
			conn.close()

	# ------------------------------------------------------------------ #
	#  Metrics                                                             #
	# ------------------------------------------------------------------ #

	def get_metrics(self):
		"""Return current metrics as a dict (for in-process use)."""
		with self.lock:
			servers_snap = dict(self.servers)
			total_queued = self._queued_count
			queue_timeouts = self._queue_timeout_count
		result = self.metrics.to_dict(servers_snap)
		result["queue_size"] = self.request_queue.qsize()
		result["total_queued"] = total_queued
		result["queue_timeouts"] = queue_timeouts
		return result

	def _metrics_writer_loop(self, interval=5):
		"""Background thread: write metrics JSON every `interval` seconds."""
		while not self.shutdown_event.is_set():
			time.sleep(interval)
			try:
				import json
				with open(self.metrics_file, "w") as f:
					json.dump(self.get_metrics(), f, indent=2)
			except Exception as e:
				logger.warning(f"Failed to write metrics file: {e}")


if __name__ == "__main__":
	import sys
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s [LB] %(levelname)s %(message)s"
	)
	algorithm = int(sys.argv[1]) if len(sys.argv) > 1 else 1
	lb = LoadBalancer(algorithm)
	if len(sys.argv) > 2:
		lb.metrics_file = sys.argv[2]
	logger.info(f"Starting Load Balancer with algorithm {algorithm} (1=RR, 2=LC, 3=Random, 4=WRR)")
	lb.start()
