import unittest
import subprocess
import time
import socket
import os

class TestServerCapacityLimit(unittest.TestCase):
	def setUp(self):
		# Start LB (algorithm 1)
		lb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/LB.py'))
		sv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/Server.py'))
		self.lb_proc = subprocess.Popen([
			'python', lb_path, '1'
		], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(0.5)
		# Start Server with capacity 1 on 127.10.0.2:9210
		self.server_proc = subprocess.Popen([
			'python', sv_path, '127.10.0.2', '9210', '1'
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

	def test_capacity_limit(self):
		print("Running test_server_capacity_limit...")
		# Open two client connections simultaneously from different client IPs
		sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock1.bind(("127.20.0.2", 0))
		sock2.bind(("127.20.0.3", 0))
		sock1.connect(('127.0.0.1', 20000))
		sock2.connect(('127.0.0.1', 20000))
		sock1.sendall(b"first")
		sock2.sendall(b"second")
		resp1 = sock1.recv(4096)
		resp2 = sock2.recv(4096)
		sock1.close()
		sock2.close()
		responses = [resp1, resp2]
		# One should be processed, one should be SERVER BUSY
		self.assertTrue(any(b"Processed by" in r for r in responses))
		self.assertTrue(any(b"SERVER BUSY" in r or b"NO AVAILABLE SERVER" in r for r in responses))
		# Expected: Only one request is processed, the other is rejected

if __name__ == "__main__":
	unittest.main()
