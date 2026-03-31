import socket
import threading
import time
import random
import logging

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
		self.lock = threading.Lock()
		self.rr_index = 0       # For Round Robin cycling
		self.shutdown_event = threading.Event()
		self.algorithm = algorithm  # 1=RR, 2=LeastConn, 3=Random, 4=WeightedRR

		# Per-server request totals used by Weighted Round Robin (reset on server join)
		self.request_totals = {}

		self.metrics = MetricsCollector()
		self.metrics_file = "lb_metrics.json"

	def start(self):
		threading.Thread(target=self.listen_for_servers, daemon=True).start()
		threading.Thread(target=self.health_check_loop, daemon=True).start()
		threading.Thread(target=self._metrics_writer_loop, daemon=True).start()
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
		with self.lock:
			server.active = max(0, server.active - 1)

	def handle_client_request(self, conn, addr):
		try:
			data = conn.recv(4096)
			server = self.select_server()
			if not server:
				conn.sendall(b"NO AVAILABLE SERVER")
				logger.warning(f"No available server for client {addr}")
				return
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
		except Exception as e:
			logger.error(f"Error handling client request: {e}")
		finally:
			conn.close()

	# ------------------------------------------------------------------ #
	#  Metrics                                                             #
	# ------------------------------------------------------------------ #

	def get_metrics(self):
		"""Return current metrics as a dict (for in-process use)."""
		with self.lock:
			servers_snap = dict(self.servers)
		return self.metrics.to_dict(servers_snap)

	def _metrics_writer_loop(self, interval=5):
		"""Background thread: write metrics JSON every `interval` seconds."""
		while not self.shutdown_event.is_set():
			time.sleep(interval)
			try:
				with self.lock:
					servers_snap = dict(self.servers)
				self.metrics.dump(self.metrics_file, servers_snap)
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
