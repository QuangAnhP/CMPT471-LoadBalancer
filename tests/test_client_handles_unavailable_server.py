import unittest
import subprocess
import time
import socket
import os

class TestClientHandlesUnavailableServer(unittest.TestCase):
	def setUp(self):
		# Start LB only (no server)
		lb_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/LB.py'))
		self.lb_proc = subprocess.Popen([
			'python', lb_path, '1'
		], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(0.5)

	def tearDown(self):
		self.lb_proc.terminate()
		self.lb_proc.wait()
		if self.lb_proc.stdout:
			self.lb_proc.stdout.close()
		if self.lb_proc.stderr:
			self.lb_proc.stderr.close()

	def test_client_handles_unavailable_server(self):
		print("Running test_client_handles_unavailable_server...")
		# Connect client (should get NO AVAILABLE SERVER) from 127.20.0.2
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			sock.sendall(b"test")
			resp = sock.recv(4096)
			self.assertIn(b"NO AVAILABLE SERVER", resp)
		# Expected: Client receives NO AVAILABLE SERVER and can handle it

if __name__ == "__main__":
	unittest.main()
