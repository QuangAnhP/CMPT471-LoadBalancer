import unittest
import subprocess
import time
import socket
import os

class TestSingleClientConnectToLB(unittest.TestCase):
	def setUp(self):
		# Start LB (algorithm 1)
		lb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/LB.py'))
		sv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/Server.py'))
		self.lb_proc = subprocess.Popen([
			'python', lb_path, '1'
		], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(0.5)
		# Start Server on 127.10.0.2:10000
		self.server_proc = subprocess.Popen([
			'python', sv_path, '127.10.0.2', '9210', '2'
		], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(0.5)

	def tearDown(self):
		self.lb_proc.terminate()
		self.server_proc.terminate()
		self.lb_proc.wait()
		self.server_proc.wait()
		if self.lb_proc.stdout:
			self.lb_proc.stdout.close()
		if self.lb_proc.stderr:
			self.lb_proc.stderr.close()
		if self.server_proc.stdout:
			self.server_proc.stdout.close()
		if self.server_proc.stderr:
			self.server_proc.stderr.close()

	def test_client_request_response(self):
		print("Running test_single_client_connect_to_LB...")
		# Connect client to LB (LB listens for clients on 127.0.0.1:20000)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			start = time.time()
			sock.sendall(b"hello")
			response = sock.recv(4096)
			end = time.time()
			latency = (end - start) * 1000
			print(f"Response: {response.decode()}, Latency: {latency:.2f} ms")
			self.assertIn(b"Processed by", response)
			self.assertLess(latency, 2000)  # Should be reasonably fast
		# Expected: Client receives a valid response and latency is measured

if __name__ == "__main__":
	unittest.main()
