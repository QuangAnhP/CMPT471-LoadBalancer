import unittest
import subprocess
import time
import socket
import os

class TestLBHandlesServerFailure(unittest.TestCase):
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
		# Close file handles to avoid ResourceWarning
		if self.lb_proc.stdout:
			self.lb_proc.stdout.close()
		if self.lb_proc.stderr:
			self.lb_proc.stderr.close()
		if self.server_proc.stdout:
			self.server_proc.stdout.close()
		if self.server_proc.stderr:
			self.server_proc.stderr.close()

	def test_lb_handles_server_failure(self):
		print("Running test_lb_handles_server_failure...")
		# Connect client (should succeed)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			sock.sendall(b"test")
			resp = sock.recv(4096)
			self.assertIn(b"Processed by", resp)
		# Kill server
		self.server_proc.terminate()
		self.server_proc.wait()
		time.sleep(0.5)
		# Connect client again (should get NO AVAILABLE SERVER or SERVER FAILURE)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			sock.sendall(b"test")
			resp = sock.recv(4096)
			self.assertTrue(b"NO AVAILABLE SERVER" in resp or b"SERVER FAILURE" in resp)
		# Expected: LB removes failed server and handles client gracefully

if __name__ == "__main__":
	unittest.main()
