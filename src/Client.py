import socket
import time
import csv as csv_mod


class Client:
	def __init__(self, lb_host, lb_port, num_requests=10, message="ping", interval=1.0,
	             persistent_connection=True):
		self.lb_host = lb_host
		self.lb_port = lb_port
		self.num_requests = num_requests
		self.message = message
		self.interval = interval            # seconds between requests
		self.latencies = []                 # per-request latency in ms (None on failure)
		self.responses = []                 # decoded response text per request (parallel to latencies)
		# persistent_connection=True: one TCP connection for all requests (original behaviour).
		# persistent_connection=False: new connection per request (needed when LB closes
		# the socket after each response, e.g. in the orchestrator).
		self.persistent_connection = persistent_connection

	def run(self):
		if self.persistent_connection:
			self._run_persistent()
		else:
			self._run_per_request()

	def _run_persistent(self):
		"""Single TCP connection reused for all requests (original behaviour)."""
		try:
			with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
				sock.connect((self.lb_host, self.lb_port))
				print(f"Connected to load balancer at {self.lb_host}:{self.lb_port}")
				for i in range(self.num_requests):
					start = time.time()
					try:
						sock.sendall(self.message.encode())
						response = sock.recv(4096)
						latency = (time.time() - start) * 1000
						decoded = response.decode()
						self.latencies.append(latency)
						self.responses.append(decoded)
						print(f"Request {i+1}: Response='{decoded}', Latency={latency:.2f} ms")
					except Exception as e:
						print(f"Error during request {i+1}: {e}")
						self.latencies.append(None)
						self.responses.append("")
					time.sleep(self.interval)
		except Exception as e:
			print(f"Could not connect to load balancer: {e}")

	def _run_per_request(self):
		"""New TCP connection for every request. Use when LB closes after each response."""
		for i in range(self.num_requests):
			start = time.time()
			try:
				with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
					sock.connect((self.lb_host, self.lb_port))
					sock.sendall(self.message.encode())
					response = sock.recv(4096)
					latency = (time.time() - start) * 1000
					decoded = response.decode()
					self.latencies.append(latency)
					self.responses.append(decoded)
					print(f"Request {i+1}: Response='{decoded}', Latency={latency:.2f} ms")
			except Exception as e:
				print(f"Error during request {i+1}: {e}")
				self.latencies.append(None)
				self.responses.append("")
			time.sleep(self.interval)

	def print_summary(self):
		valid = [l for l in self.latencies if l is not None]
		total = len(self.latencies)
		if not valid:
			print(f"\nNo successful requests (0/{total}).")
			return

		s = sorted(valid)
		n = len(s)
		avg = sum(valid) / n
		p50 = s[n // 2]
		p95 = s[min(int(n * 0.95), n - 1)]
		p99 = s[min(int(n * 0.99), n - 1)]

		# Jitter: RMS of consecutive inter-request latency differences
		diffs = [abs(valid[i + 1] - valid[i]) for i in range(len(valid) - 1)]
		jitter = (sum(d ** 2 for d in diffs) / len(diffs)) ** 0.5 if diffs else 0.0

		print(f"\nRequests: {n}/{total} successful")
		print(f"Latency  — Avg: {avg:.1f} ms  P50: {p50:.1f} ms  P95: {p95:.1f} ms  P99: {p99:.1f} ms")
		print(f"Jitter   — {jitter:.1f} ms (RMS of consecutive differences)")

	def export_csv(self, filename):
		"""Write per-request latencies to a CSV file."""
		with open(filename, "w", newline="") as f:
			writer = csv_mod.writer(f)
			writer.writerow(["request_num", "latency_ms"])
			for i, l in enumerate(self.latencies):
				writer.writerow([i + 1, f"{l:.3f}" if l is not None else ""])


if __name__ == "__main__":
	import argparse
	parser = argparse.ArgumentParser(description="Load balancer test client.")
	parser.add_argument("--host", default="127.0.0.1")
	parser.add_argument("--port", type=int, default=20000)
	parser.add_argument("--requests", type=int, default=10)
	parser.add_argument("--interval", type=float, default=0.5)
	parser.add_argument("--message", default="ping")
	parser.add_argument("--output-csv", default=None, help="Write latencies to this CSV file")
	args = parser.parse_args()

	client = Client(args.host, args.port, args.requests, args.message, args.interval,
	                persistent_connection=False)
	client.run()
	client.print_summary()
	if args.output_csv:
		client.export_csv(args.output_csv)
		print(f"Latencies written to {args.output_csv}")
