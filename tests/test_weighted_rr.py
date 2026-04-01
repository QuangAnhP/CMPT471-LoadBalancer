"""
test_weighted_rr.py — Unit tests for Weighted Round Robin (algorithm=4).
"""
import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from LB import LoadBalancer, ServerInfo


class TestWeightedRR(unittest.TestCase):

    def setUp(self):
        self.lb = LoadBalancer(algorithm=4)
        self.s1 = ServerInfo('127.10.0.2', 8001, capacity=2)
        self.s2 = ServerInfo('127.10.0.3', 8002, capacity=4)
        self.lb.servers[('127.10.0.2', 8001)] = self.s1
        self.lb.servers[('127.10.0.3', 8002)] = self.s2
        self.lb.request_totals[('127.10.0.2', 8001)] = 0
        self.lb.request_totals[('127.10.0.3', 8002)] = 0

    def test_wrr_selects_available_server(self):
        """WRR must return one of the registered servers."""
        s = self.lb.select_server()
        self.assertIn(s, [self.s1, self.s2])

    def test_wrr_proportional_distribution(self):
        """
        Over many selections, the server with double capacity should receive
        roughly double the requests (within 10% tolerance).
        """
        for _ in range(600):
            s = self.lb.select_server()
            if s:
                self.lb.release_server(s)

        total = sum(self.lb.request_totals.values())
        share_s1 = self.lb.request_totals[('127.10.0.2', 8001)] / total
        share_s2 = self.lb.request_totals[('127.10.0.3', 8002)] / total

        # cap=2 → ~33%, cap=4 → ~67%
        self.assertAlmostEqual(share_s1, 1 / 3, delta=0.10,
            msg=f"WRR: cap=2 server got {share_s1:.3f}, expected ~0.333")
        self.assertAlmostEqual(share_s2, 2 / 3, delta=0.10,
            msg=f"WRR: cap=4 server got {share_s2:.3f}, expected ~0.667")

    def test_wrr_increments_request_totals(self):
        """Each select_server() call must increment request_totals for the chosen server."""
        before = sum(self.lb.request_totals.values())
        self.lb.select_server()
        after = sum(self.lb.request_totals.values())
        self.assertEqual(after, before + 1)

    def test_wrr_respects_capacity(self):
        """WRR must not select a server that is at full capacity."""
        self.s1.active = self.s1.capacity  # s1 full
        self.s2.active = self.s2.capacity  # s2 full
        result = self.lb.select_server()
        self.assertIsNone(result)

    def test_wrr_skips_full_server(self):
        """When one server is full, WRR must route all requests to the other."""
        self.s1.active = self.s1.capacity  # s1 full, s2 available
        for _ in range(10):
            s = self.lb.select_server()
            self.assertEqual(s, self.s2,
                "WRR should only pick the server that still has capacity")
            self.lb.release_server(s)

    def test_wrr_resets_totals_on_join(self):
        """When a server rejoins, its WRR request total resets to 0."""
        # Simulate some traffic
        self.lb.request_totals[('127.10.0.2', 8001)] = 999
        # Server sends a JOIN (re-registration)
        import unittest.mock as mock
        mock_conn = mock.MagicMock()
        mock_conn.recv.return_value = b"JOIN 127.10.0.2 8001 2"
        self.lb.handle_server_message(mock_conn)
        self.assertEqual(self.lb.request_totals.get(('127.10.0.2', 8001)), 0,
            "WRR totals should reset on server re-join so it can ramp up again")


if __name__ == "__main__":
    unittest.main(verbosity=2)
