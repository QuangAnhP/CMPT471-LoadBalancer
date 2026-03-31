import unittest
from unittest.mock import patch, MagicMock
import threading
import time
import signal
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from Server import Server

class TestServer(unittest.TestCase):
	def setUp(self):
		# Use dummy addresses and ports
		self.lb_host = '127.0.0.1'
		self.lb_port = 9999
		self.server_host = '127.0.0.1'
		self.server_port = 8888
		self.capacity = 2
		self.server = Server(self.server_host, self.server_port, self.capacity)

	@patch('socket.socket')
	def test_register_with_lb(self, mock_socket):
		print("Running test_register_with_lb...")
		mock_sock = MagicMock()
		mock_socket.return_value.__enter__.return_value = mock_sock
		self.server.register_with_lb()
		# Should send JOIN message
		msg = f"JOIN {self.server_host} {self.server_port} {self.capacity}".encode()
		mock_sock.sendall.assert_called_with(msg)
		# Expected: Registration message sent

	@patch('socket.socket')
	def test_deregister_with_lb(self, mock_socket):
		print("Running test_deregister_with_lb...")
		mock_sock = MagicMock()
		mock_socket.return_value.__enter__.return_value = mock_sock
		self.server.deregister_with_lb()
		msg = f"LEAVE {self.server_host} {self.server_port}".encode()
		mock_sock.sendall.assert_called_with(msg)
		# Expected: Deregistration message sent

	def test_capacity_limit(self):
		print("Running test_capacity_limit...")
		# Simulate connections using dummy sockets
		class DummyConn:
			def __init__(self):
				self.sent = []
				self.closed = False
			def sendall(self, data):
				self.sent.append(data)
			def recv(self, bufsize):
				return b"test"
			def close(self):
				self.closed = True

		# Fill up capacity
		conns = [DummyConn() for _ in range(self.capacity)]
		threads = []
		for conn in conns:
			t = threading.Thread(target=self.server.process_request, args=(conn, ('127.0.0.1', 0)))
			threads.append(t)
			t.start()
		time.sleep(0.1)  # Let threads increment active_requests

		# Now try one more (should be rejected)
		extra_conn = DummyConn()
		self.server.process_request(extra_conn, ('127.0.0.1', 1))
		self.assertIn(b"SERVER BUSY", extra_conn.sent)
		self.assertTrue(extra_conn.closed)
		# Expected: Extra request is rejected with SERVER BUSY

		# Clean up
		for t in threads:
			t.join()

	def test_active_requests_decrement(self):
		print("Running test_active_requests_decrement...")
		class DummyConn:
			def __init__(self):
				self.closed = False
			def sendall(self, data):
				pass
			def recv(self, bufsize):
				return b"test"
			def close(self):
				self.closed = True

		conn = DummyConn()
		self.server.active_requests = 0
		self.server.process_request(conn, ('127.0.0.1', 2))
		self.assertEqual(self.server.active_requests, 0)
		# Expected: active_requests decremented after processing

	def test_handle_shutdown(self):
		print("Running test_handle_shutdown...")
		with patch.object(self.server, 'deregister_with_lb') as mock_dereg:
			self.server.handle_shutdown(signal.SIGINT, None)
			mock_dereg.assert_called_once()
			self.assertTrue(self.server.shutdown_event.is_set())
		# Expected: Server deregisters and sets shutdown event

if __name__ == "__main__":
	unittest.main()