import socket
import threading
import sys
import signal
import time

class Server:
	def __init__(self, server_host, server_port, capacity=5):
		self.lb_host = "127.0.0.1"
		self.lb_port = 10000
		self.server_host = server_host  # e.g., 127.10.0.X
		self.server_port = server_port
		self.capacity = capacity
		self.active_requests = 0
		self.lock = threading.Lock()
		self.shutdown_event = threading.Event()
		signal.signal(signal.SIGINT, self.handle_shutdown)
		signal.signal(signal.SIGTERM, self.handle_shutdown)

	def register_with_lb(self, timeout=5):
		# Register with the load balancer (simple protocol: send 'JOIN <host> <port> <capacity>')
		start = time.time()
		while time.time() - start < timeout:
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.settimeout(1)
					sock.connect((self.lb_host, self.lb_port))
					msg = f"JOIN {self.server_host} {self.server_port} {self.capacity}"
					sock.sendall(msg.encode())
					print(f"Registered with load balancer: {msg}")
					return True
			except Exception as e:
				print(f"Retrying registration with load balancer: {e}")
				time.sleep(0.1)
		print(f"Failed to register with load balancer after {timeout} seconds.")
		return False

	def deregister_with_lb(self):
		# Deregister with the load balancer (simple protocol: send 'LEAVE <host> <port>')
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.connect((self.lb_host, self.lb_port))
				msg = f"LEAVE {self.server_host} {self.server_port}"
				sock.sendall(msg.encode())
				print(f"Deregistered from load balancer: {msg}")
		except Exception as e:
			print(f"Failed to deregister from load balancer: {e}")

	def handle_shutdown(self, signum, frame):
		print("Shutting down server...")
		self.deregister_with_lb()
		self.shutdown_event.set()

	def process_request(self, conn, addr):
		with self.lock:
			if self.active_requests >= self.capacity:
				conn.sendall(b"SERVER BUSY")
				conn.close()
				print(f"Rejected request from {addr}: capacity full")
				return
			self.active_requests += 1
		try:
			data = conn.recv(4096)
			# Simulate processing time
			time.sleep(1)
			response = f"Processed by {self.server_host}:{self.server_port}"
			conn.sendall(response.encode())
			print(f"Processed request from {addr}")
		except Exception as e:
			print(f"Error processing request from {addr}: {e}")
		finally:
			with self.lock:
				self.active_requests -= 1
			conn.close()

	def run(self):
		if not self.register_with_lb():
			print("Server exiting due to registration failure.")
			return
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
			server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			server_sock.bind((self.server_host, self.server_port))
			server_sock.listen()
			print(f"Server listening on {self.server_host}:{self.server_port} (capacity: {self.capacity})")
			while not self.shutdown_event.is_set():
				try:
					server_sock.settimeout(1.0)
					conn, addr = server_sock.accept()
					threading.Thread(target=self.process_request, args=(conn, addr), daemon=True).start()
				except socket.timeout:
					continue
				except Exception as e:
					print(f"Server error: {e}")
		print("Server stopped.")

if __name__ == "__main__":
	# Example usage: python Server.py <SERVER_HOST> <SERVER_PORT> [CAPACITY]
	if len(sys.argv) < 3:
		print("Usage: python Server.py <SERVER_HOST> <SERVER_PORT> [CAPACITY]")
		sys.exit(1)
	server_host = sys.argv[1]  # e.g., 127.10.0.2
	server_port = int(sys.argv[2])
	capacity = int(sys.argv[3]) if len(sys.argv) > 3 else 5
	server = Server(server_host, server_port, capacity)
	server.run()
