import socket
import threading
import time

class ServerInfo:
	def __init__(self, host, port, capacity):
		self.host = host
		self.port = port
		self.capacity = capacity
		self.active = 0  # Number of active requests
		self.last_seen = time.time()

import random

class LoadBalancer:
	def __init__(self, algorithm=1):
		self.lb_host = "127.0.0.1"
		self.server_port = 10000
		self.client_port = 20000
		self.servers = {}  # (host, port) -> ServerInfo
		self.lock = threading.Lock()
		self.rr_index = 0  # For round robin
		self.shutdown_event = threading.Event()
		self.algorithm = algorithm  # 1=RR, 2=LeastConn, 3=Random

	def start(self):
		threading.Thread(target=self.listen_for_servers, daemon=True).start()
		threading.Thread(target=self.health_check_loop, daemon=True).start()
		self.listen_for_clients()

	def health_check_loop(self, interval=2):
		while not self.shutdown_event.is_set():
			time.sleep(interval)
			to_remove = []
			with self.lock:
				for (host, port), server in list(self.servers.items()):
					if not self.ping_server(server):
						print(f"Health check failed: removing server {host}:{port}")
						to_remove.append((host, port))
			with self.lock:
				for key in to_remove:
					self.servers.pop(key, None)

	def ping_server(self, server, timeout=1):
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.settimeout(timeout)
				sock.connect((server.host, server.port))
				# Optionally, send a ping message and expect a response
				return True
		except Exception:
			return False

	def listen_for_servers(self):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.bind((self.lb_host, self.server_port))
			sock.listen()
			print(f"Load Balancer listening for servers on {self.lb_host}:{self.server_port}")
			while not self.shutdown_event.is_set():
				try:
					sock.settimeout(1.0)
					conn, addr = sock.accept()
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
				print(f"Server joined: {host}:{port} (capacity {capacity})")
			elif parts[0] == "LEAVE" and len(parts) == 3:
				host, port = parts[1], int(parts[2])
				with self.lock:
					self.servers.pop((host, port), None)
				print(f"Server left: {host}:{port}")
		except Exception as e:
			print(f"Error handling server message: {e}")
		finally:
			conn.close()

	def listen_for_clients(self):
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			sock.bind((self.lb_host, self.client_port))
			sock.listen()
			print(f"Load Balancer listening for clients on {self.lb_host}:{self.client_port}")
			while not self.shutdown_event.is_set():
				try:
					sock.settimeout(1.0)
					conn, addr = sock.accept()
					threading.Thread(target=self.handle_client_request, args=(conn, addr), daemon=True).start()
				except socket.timeout:
					continue

	def select_server(self):
		with self.lock:
			available = [(k, s) for k, s in self.servers.items() if s.active < s.capacity]
			if not available:
				return None
			if self.algorithm == 1:
				# Round Robin
				idx = self.rr_index % len(available)
				self.rr_index += 1
				key, server = available[idx]
			elif self.algorithm == 2:
				# Least Connections
				key, server = min(available, key=lambda item: item[1].active)
			elif self.algorithm == 3:
				# Random
				key, server = random.choice(available)
			else:
				# Default to Round Robin
				idx = self.rr_index % len(available)
				self.rr_index += 1
				key, server = available[idx]
			server.active += 1
			server.last_seen = time.time()
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
				print(f"No available server for client {addr}")
				return
			# Forward request to server
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sock:
				try:
					s_sock.connect((server.host, server.port))
					s_sock.sendall(data)
					response = s_sock.recv(4096)
					conn.sendall(response)
					print(f"Forwarded request from {addr} to {server.host}:{server.port}")
				except Exception as e:
					conn.sendall(b"SERVER FAILURE")
					print(f"Server failure: {server.host}:{server.port} - {e}")
					with self.lock:
						self.servers.pop((server.host, server.port), None)
				finally:
					self.release_server(server)
		except Exception as e:
			print(f"Error handling client request: {e}")
		finally:
			conn.close()

if __name__ == "__main__":
	# Example usage: python LB.py [ALGORITHM]
	import sys
	algorithm = int(sys.argv[1]) if len(sys.argv) > 1 else 1
	lb = LoadBalancer(algorithm)
	print(f"Starting Load Balancer with algorithm {algorithm}")
	lb.start()
