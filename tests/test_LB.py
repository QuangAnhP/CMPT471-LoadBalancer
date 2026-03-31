import unittest
from unittest.mock import patch, MagicMock
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from LB import LoadBalancer, ServerInfo

class TestLoadBalancer(unittest.TestCase):
	def setUp(self):
		self.lb = LoadBalancer(algorithm=1)
		# Add some servers manually (use 127.10.0.X)
		self.s1 = ServerInfo('127.10.0.2', 8001, 2)
		self.s2 = ServerInfo('127.10.0.3', 8002, 3)
		self.lb.servers[('127.10.0.2', 8001)] = self.s1
		self.lb.servers[('127.10.0.3', 8002)] = self.s2

	def test_server_join(self):
		print("Running test_server_join...")
		# Simulate JOIN message
		with patch('socket.socket') as mock_socket:
			mock_conn = MagicMock()
			mock_conn.recv.return_value = b"JOIN 127.10.0.4 8003 4"
			mock_socket.return_value.__enter__.return_value = mock_conn
			self.lb.handle_server_message(mock_conn)
			self.assertIn(('127.10.0.4', 8003), self.lb.servers)
		# Expected: Server 127.10.0.4:8003 is added

	def test_server_leave(self):
		print("Running test_server_leave...")
		# Simulate LEAVE message
		with patch('socket.socket') as mock_socket:
			mock_conn = MagicMock()
			mock_conn.recv.return_value = b"LEAVE 127.10.0.2 8001"
			mock_socket.return_value.__enter__.return_value = mock_conn
			self.lb.handle_server_message(mock_conn)
			self.assertNotIn(('127.10.0.2', 8001), self.lb.servers)
		# Expected: Server 127.10.0.2:8001 is removed

	def test_round_robin_selection(self):
		print("Running test_round_robin_selection...")
		self.lb.algorithm = 1
		# Both servers have capacity
		s_first = self.lb.select_server()
		s_second = self.lb.select_server()
		self.assertNotEqual(s_first, s_second)
		# Expected: Alternates between servers

	def test_least_connections_selection(self):
		print("Running test_least_connections_selection...")
		self.lb.algorithm = 2
		self.s1.active = 1
		self.s2.active = 0
		s = self.lb.select_server()
		self.assertEqual(s, self.s2)
		# Expected: Chooses server with least active connections

	def test_random_selection(self):
		print("Running test_random_selection...")
		self.lb.algorithm = 3
		# Should return one of the available servers
		s = self.lb.select_server()
		self.assertIn(s, [self.s1, self.s2])
		# Expected: Returns a random server

	def test_no_available_server(self):
		print("Running test_no_available_server...")
		self.s1.active = self.s1.capacity
		self.s2.active = self.s2.capacity
		s = self.lb.select_server()
		self.assertIsNone(s)
		# Expected: Returns None when all servers are at capacity

	def test_release_server(self):
		print("Running test_release_server...")
		self.s1.active = 2
		self.lb.release_server(self.s1)
		self.assertEqual(self.s1.active, 1)
		# Expected: Decrements active count, but not below zero
		self.lb.release_server(self.s1)
		self.lb.release_server(self.s1)
		self.assertEqual(self.s1.active, 0)

if __name__ == "__main__":
	unittest.main()
