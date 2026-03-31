import socket
import time

class Client:
	def __init__(self, lb_host, lb_port, num_requests=10, message="ping", interval=1.0):
		self.lb_host = lb_host
		self.lb_port = lb_port
		self.num_requests = num_requests
		self.message = message
		self.interval = interval	# number of seconds between requests
		self.latencies = []			# in ms

	def run(self):
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.connect((self.lb_host, self.lb_port))
				print(f"Connected to load balancer at {self.lb_host}:{self.lb_port}")
				for i in range(self.num_requests):
					start = time.time()
					try:
						sock.sendall(self.message.encode())
						response = sock.recv(4096)
						end = time.time()
						latency = (end - start) * 1000
						self.latencies.append(latency)
						print(f"Request {i+1}: Response='{response.decode()}', Latency={latency:.2f} ms")
					except Exception as e:
						print(f"Error during request {i+1}: {e}")
						self.latencies.append(None)
					time.sleep(self.interval)
		except Exception as e:
			print(f"Could not connect to load balancer: {e}")

	def print_summary(self):
		valid_latencies = [l for l in self.latencies if l is not None]
		if valid_latencies:
			avg = sum(valid_latencies) / len(valid_latencies)
			print(f"\nAverage latency: {avg:.2f} ms over {len(valid_latencies)} successful requests.")
		else:
			print("\nNo successful requests.")