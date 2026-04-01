"""
MetricsCollector: Aggregates per-server request counts, utilization snapshots,
and completion timestamps. Computes derived metrics: throughput, weighted fairness
(Jain's index), and per-server average utilization.
"""
import time
import json
import threading


class MetricsCollector:
    """Thread-safe collector for load balancer operational metrics."""

    def __init__(self):
        self._lock = threading.Lock()
        # (host, port) -> total requests forwarded to this server (never reset)
        self.request_counts = {}
        # Float timestamps of completed requests, used for throughput window
        self.completion_times = []
        # List of (timestamp, {server_label: utilization_fraction}) snapshots
        self.utilization_log = []

    def record_request(self, server_key):
        """Increment request count for server_key = (host, port)."""
        with self._lock:
            self.request_counts[server_key] = self.request_counts.get(server_key, 0) + 1

    def record_completion(self):
        """Record a completed request timestamp (used for throughput)."""
        with self._lock:
            self.completion_times.append(time.time())

    def snapshot_utilization(self, servers):
        """
        Take a utilization snapshot.
        servers: dict of (host, port) -> ServerInfo with .active and .capacity fields.
        """
        snapshot = {
            f"{s.host}:{s.port}": round(s.active / s.capacity, 4) if s.capacity > 0 else 0.0
            for s in servers.values()
        }
        with self._lock:
            self.utilization_log.append((time.time(), snapshot))

    def throughput(self, window=10.0):
        """Requests completed per second over the last `window` seconds."""
        now = time.time()
        with self._lock:
            recent = sum(1 for t in self.completion_times if t >= now - window)
        return round(recent / window, 3)

    def weighted_fairness(self, servers):
        """
        Weighted Jain's fairness index comparing actual request distribution to
        the capacity-proportional ideal. Returns a value in (0, 1], where 1.0
        means perfectly fair distribution.

        For each server i:
          ratio_i = actual_fraction_i / expected_fraction_i
          expected_fraction_i = capacity_i / total_capacity

        Jain's index on ratios: (sum(ratio_i))^2 / (n * sum(ratio_i^2))
        """
        with self._lock:
            counts = dict(self.request_counts)
        if not counts or not servers:
            return None
        total_requests = sum(counts.values())
        total_capacity = sum(s.capacity for s in servers.values())
        if total_requests == 0 or total_capacity == 0:
            return None
        ratios = []
        for key, s in servers.items():
            actual = counts.get(key, 0) / total_requests
            expected = s.capacity / total_capacity
            if expected > 0:
                ratios.append(actual / expected)
        if not ratios:
            return None
        n = len(ratios)
        return round((sum(ratios) ** 2) / (n * sum(r ** 2 for r in ratios)), 4)

    def avg_utilization_per_server(self):
        """Average utilization fraction (0–1) per server across all snapshots."""
        with self._lock:
            log = list(self.utilization_log)
        if not log:
            return {}
        totals, counts = {}, {}
        for _, snap in log:
            for label, util in snap.items():
                totals[label] = totals.get(label, 0) + util
                counts[label] = counts.get(label, 0) + 1
        return {label: round(totals[label] / counts[label], 4) for label in totals}

    def to_dict(self, servers=None):
        """Return all metrics as a plain JSON-serializable dict."""
        with self._lock:
            rc = {f"{k[0]}:{k[1]}": v for k, v in self.request_counts.items()}
            total = len(self.completion_times)
        result = {
            "request_counts": rc,
            "total_completions": total,
            "throughput_rps": self.throughput(),
            "avg_utilization": self.avg_utilization_per_server(),
        }
        if servers is not None:
            result["weighted_fairness"] = self.weighted_fairness(servers)
        return result

    def dump(self, filepath, servers=None):
        """Write all metrics to a JSON file at filepath."""
        with open(filepath, "w") as f:
            json.dump(self.to_dict(servers), f, indent=2)
