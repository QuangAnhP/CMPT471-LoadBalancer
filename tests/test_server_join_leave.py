import unittest
import subprocess
import time
import socket
import os

class TestServerJoinLeave(unittest.TestCase):
	def setUp(self):
		# Start LB (algorithm 1)
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

	def test_server_join_leave(self):
		print("Running test_server_join_leave...")
		# Start server (join) on 127.10.0.2:9220
		sv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/Server.py'))
		server_proc = subprocess.Popen([
			'python', sv_path, '127.10.0.2', '9220', '2'
		], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		time.sleep(0.5)
		# Connect client (should succeed) from 127.20.0.2
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			sock.sendall(b"test")
			resp = sock.recv(4096)
			self.assertIn(b"Processed by", resp)
		# Stop server (leave)
		server_proc.terminate()
		server_proc.wait()
		if server_proc.stdout:
			server_proc.stdout.close()
		if server_proc.stderr:
			server_proc.stderr.close()
		time.sleep(0.5)
		# Connect client again (should fail: no available server)
		with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
			sock.bind(("127.20.0.2", 0))
			sock.connect(('127.0.0.1', 20000))
			sock.sendall(b"test")
			resp = sock.recv(4096)
			self.assertTrue(
				b"NO AVAILABLE SERVER" in resp or b"SERVER FAILURE" in resp,
				f"Unexpected response: {resp}"
			)
		# Expected: Server can join and leave, LB updates registry

if __name__ == "__main__":
	unittest.main()
