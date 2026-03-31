"""
test_scalability.py — Measures select_server() performance vs. server pool size,
and compares all four algorithms at 100 servers.

Run: python tests/test_scalability.py
"""
import unittest
import time
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from LB import LoadBalancer, ServerInfo


class TestScalability(unittest.TestCase):

    def _make_lb(self, n, algorithm):
        """Create a LoadBalancer with n servers pre-registered (no real sockets)."""
        lb = LoadBalancer(algorithm=algorithm)
        for i in range(n):
            key = ('127.0.0.1', 9000 + i)
            lb.servers[key] = ServerInfo('127.0.0.1', 9000 + i, capacity=10)
            lb.request_totals[key] = 0
        return lb

    def _avg_select_time_ms(self, lb, iterations=2000):
        """Return average time per select_server() call in milliseconds."""
        start = time.perf_counter()
        for _ in range(iterations):
            server = lb.select_server()
            if server:
                lb.release_server(server)
        elapsed = time.perf_counter() - start
        return (elapsed / iterations) * 1000

    # ------------------------------------------------------------------ #

    def test_lc_scales_with_server_count(self):
        """
        Least Connections is O(N) — verify it stays under 100 ms/call even at
        500 servers, and print a scaling table.
        """
        print("\n=== select_server() scaling — Least Connections (algorithm=2) ===")
        print(f"  {'Servers':>8}  {'Avg time (ms)':>14}")
        print(f"  {'-'*25}")
        for n in [10, 50, 100, 500]:
            lb = self._make_lb(n, algorithm=2)
            avg_ms = self._avg_select_time_ms(lb)
            print(f"  {n:>8}  {avg_ms:>14.4f}")
            self.assertLess(avg_ms, 100,
                f"select_server() too slow at {n} servers: {avg_ms:.2f} ms")

    def test_algorithm_comparison_at_100_servers(self):
        """
        Compare average select_server() call time for all four algorithms
        at a pool of 100 servers.
        """
        n = 100
        print(f"\n=== select_server() time at {n} servers ===")
        print(f"  {'Algorithm':<22}  {'Avg time (ms)':>14}")
        print(f"  {'-'*38}")
        for algo, name in [
            (1, "Round Robin"),
            (2, "Least Connections"),
            (3, "Random"),
            (4, "Weighted RR"),
        ]:
            lb = self._make_lb(n, algorithm=algo)
            avg_ms = self._avg_select_time_ms(lb)
            print(f"  {name:<22}  {avg_ms:>14.4f}")
            self.assertLess(avg_ms, 100,
                f"{name} select_server() too slow at {n} servers: {avg_ms:.2f} ms")

    def test_distribution_weighted_rr(self):
        """
        Weighted RR should assign roughly twice as many requests to a server
        with double the capacity.
        """
        lb = LoadBalancer(algorithm=4)
        lb.servers[('127.0.0.1', 9001)] = ServerInfo('127.0.0.1', 9001, capacity=2)
        lb.servers[('127.0.0.1', 9002)] = ServerInfo('127.0.0.1', 9002, capacity=4)
        lb.request_totals[('127.0.0.1', 9001)] = 0
        lb.request_totals[('127.0.0.1', 9002)] = 0

        for _ in range(600):
            s = lb.select_server()
            if s:
                lb.release_server(s)

        total = sum(lb.request_totals.values())
        ratio_9001 = lb.request_totals[('127.0.0.1', 9001)] / total
        ratio_9002 = lb.request_totals[('127.0.0.1', 9002)] / total

        # capacity 2:4 → expected fractions ~0.333 and ~0.667
        self.assertAlmostEqual(ratio_9001, 1/3, delta=0.05,
            msg=f"WRR: server with cap=2 got {ratio_9001:.3f}, expected ~0.333")
        self.assertAlmostEqual(ratio_9002, 2/3, delta=0.05,
            msg=f"WRR: server with cap=4 got {ratio_9002:.3f}, expected ~0.667")


if __name__ == "__main__":
    unittest.main(verbosity=2)
