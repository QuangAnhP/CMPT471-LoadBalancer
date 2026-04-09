import socket
import threading
import signal
import time
import random
import argparse
import logging

logger = logging.getLogger(__name__)



class Server:
	def __init__(self, server_host, server_port, capacity=5, processing_delay="constant"):
		self.lb_host = "127.0.0.1"
		self.lb_port = 10000
		self.server_host = server_host  # e.g., 127.10.0.X
		self.server_port = server_port
		self.capacity = capacity
		self.active_requests = 0
		self.lock = threading.Lock()
		self.shutdown_event = threading.Event()

		# Processing profile controls simulated processing time per request:
		#   constant - always 1.0s (original behaviour)
		#   variable - Gaussian(mean=1.0s, std=0.3s), clamped to [0.05, 5.0]
		#   bimodal  - 80% fast (0.2s) / 20% slow (3.0s) to simulate variable processing times
		self.processing_delay = processing_delay

		signal.signal(signal.SIGINT, self.handle_shutdown)
		signal.signal(signal.SIGTERM, self.handle_shutdown)

	# ------------------------------------------------------------------ #
	#  Registration                                                        #
	# ------------------------------------------------------------------ #

	def register_with_lb(self, timeout=5):
		"""Send JOIN message to the load balancer. Retries until timeout."""
		start = time.time()
		while time.time() - start < timeout:
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.settimeout(1)
					sock.connect((self.lb_host, self.lb_port))
					msg = f"JOIN {self.server_host} {self.server_port} {self.capacity}"
					sock.sendall(msg.encode())
					logger.info(f"Registered with load balancer: {msg}")
					return True
			except Exception as e:
				logger.debug(f"Retrying registration: {e}")
				time.sleep(0.1)
		logger.error(f"Failed to register with load balancer after {timeout}s.")
		return False

	def deregister_with_lb(self):
		"""Send LEAVE message to the load balancer."""
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.connect((self.lb_host, self.lb_port))
				msg = f"LEAVE {self.server_host} {self.server_port}"
				sock.sendall(msg.encode())
				logger.info(f"Deregistered from load balancer: {msg}")
		except Exception as e:
			logger.warning(f"Failed to deregister from load balancer: {e}")

	def handle_shutdown(self, _signum, _frame):
		logger.info("Shutting down server...")
		self.deregister_with_lb()
		self.shutdown_event.set()

	# ------------------------------------------------------------------ #
	#  Request processing                                                  #
	# ------------------------------------------------------------------ #

	def _processing_time(self):
		"""Return simulated processing time in seconds based on processing_delay (not traffic load)."""
		if self.processing_delay == "variable":
			# Gaussian around 1s; clamp to reasonable bounds
			return max(0.05, min(5.0, random.gauss(1.0, 0.3)))
		elif self.processing_delay == "bimodal":
			# 20% of requests are slow (3.0s), 80% are fast (0.2s)
			return 3.0 if random.random() < 0.2 else 0.2
		else:
			# constant (default): always 1.0s
			return 1.0

	def process_request(self, conn, addr):
		try:
			data = conn.recv(4096)
		except Exception as e:
			logger.error(f"Failed to read from {addr}: {e}")
			conn.close()
			return

		# Health check: respond immediately without counting against capacity
		if data == b"PING":
			try:
				conn.sendall(b"PONG")
			finally:
				conn.close()
			return

		with self.lock:
			if self.active_requests >= self.capacity:
				conn.sendall(b"SERVER BUSY")
				conn.close()
				logger.warning(f"Rejected request from {addr}: capacity full")
				return
			self.active_requests += 1
		try:
			proc_time = self._processing_time()
			time.sleep(proc_time)
			response = f"Processed by {self.server_host}:{self.server_port}"
			conn.sendall(response.encode())
			logger.info(f"Processed request from {addr} (processing_time={proc_time:.2f}s)")
		except Exception as e:
			logger.error(f"Error processing request from {addr}: {e}")
		finally:
			with self.lock:
				self.active_requests -= 1
			conn.close()

	# ------------------------------------------------------------------ #
	#  Main loop                                                           #
	# ------------------------------------------------------------------ #

	def run(self):
		if not self.register_with_lb():
			logger.error("Server exiting due to registration failure.")
			return
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
			server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			server_sock.bind((self.server_host, self.server_port))
			server_sock.listen()
			logger.info(
				f"Server listening on {self.server_host}:{self.server_port} "
				f"(capacity={self.capacity}, delay profile={self.processing_delay})"
			)
			while not self.shutdown_event.is_set():
				try:
					server_sock.settimeout(1.0)
					conn, addr = server_sock.accept()
					threading.Thread(target=self.process_request, args=(conn, addr), daemon=True).start()
				except socket.timeout:
					continue
				except Exception as e:
					logger.error(f"Server error: {e}")
		logger.info("Server stopped.")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Backend server for the load balancer.")
	parser.add_argument("server_host", help="Host address for this server (e.g. 127.10.0.2)")
	parser.add_argument("server_port", type=int, help="Port for this server to listen on")
	parser.add_argument("capacity", type=int, nargs="?", default=5,
						help="Max concurrent requests (default: 5)")
	parser.add_argument("--processing-delay", default="constant", choices=["constant", "variable", "bimodal"],
						help="Simulated different processing time delays (default: constant)")
	args = parser.parse_args()

	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s [Server %(process)d] %(levelname)s %(message)s"
	)

	server = Server(args.server_host, args.server_port, args.capacity, args.processing_delay)
	server.run()
