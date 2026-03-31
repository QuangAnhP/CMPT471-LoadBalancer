import unittest
import socket
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from Client import Client

class DummySocket:
	"""A dummy socket for testing Client logic without real network."""
	def __init__(self):
		self.sent_data = []
		self.to_receive = [b'pong'] * 10
		self.closed = False
	def connect(self, addr):
		self.connected_addr = addr
	def sendall(self, data):
		self.sent_data.append(data)
	def recv(self, bufsize):
		if self.to_receive:
			return self.to_receive.pop(0)
		return b''
	def close(self):
		self.closed = True
	def __enter__(self):
		return self
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()


class TestClient(unittest.TestCase):
	def test_client_run_success(self):
		"""
		Test: Client successfully sends and receives responses for all requests.
		Expected: All latencies are recorded (not None), length matches num_requests.
		"""
		print("\nRunning test_client_run_success...")
		# Patch socket.socket to use DummySocket
		orig_socket = socket.socket
		socket.socket = lambda *a, **kw: DummySocket()
		client = Client('127.0.0.1', 10000, num_requests=3, message="ping", interval=0)
		client.run()
		self.assertEqual(len(client.latencies), 3)
		self.assertTrue(all(l is not None for l in client.latencies))
		socket.socket = orig_socket

	def test_client_connection_failure(self):
		"""
		Test: Client fails to connect to the load balancer (connection refused).
		Expected: No requests are sent, latencies list remains empty.
		"""
		print("\nRunning test_client_connection_failure...")
		class FailingSocket(DummySocket):
			def connect(self, addr):
				raise ConnectionRefusedError()
		orig_socket = socket.socket
		socket.socket = lambda *a, **kw: FailingSocket()
		client = Client('127.0.0.1', 10000, num_requests=2)
		client.run()
		self.assertEqual(client.latencies, [])
		socket.socket = orig_socket

	def test_client_request_failure(self):
		"""
		Test: Client connects, but sending a request fails every time.
		Expected: Latencies list has correct length, but all entries are None.
		"""
		print("\nRunning test_client_request_failure...")
		class FailingSendSocket(DummySocket):
			def sendall(self, data):
				raise Exception("Send failed")
		orig_socket = socket.socket
		socket.socket = lambda *a, **kw: FailingSendSocket()
		client = Client('127.0.0.1', 10000, num_requests=2)
		client.run()
		self.assertEqual(len(client.latencies), 2)
		self.assertTrue(all(l is None for l in client.latencies))
		socket.socket = orig_socket

if __name__ == "__main__":
	unittest.main()
