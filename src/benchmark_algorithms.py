"""
Benchmark all 4 LB algorithms under high concurrent load.

Setup:
  - 2 backend servers with UNEQUAL capacity (1 and 3 slots)
  - 40 concurrent requests fired at once
  - Each request takes ~1s to process (flat profile)
  - Total server capacity = 4 slots → forces heavy queueing

Unequal capacity makes algorithm differences clearly visible:
  - Round Robin / Random: split 50/50 → overload the weak server
  - Least Connections: adapts dynamically to actual load
  - Weighted Round Robin: splits 25/75 (proportional to capacity) → ideal

Run from src/:
    python benchmark_algorithms.py
"""
import socket
import threading
import time
import sys
import os
import logging

# Suppress internal LB/Server INFO logs so benchmark output is readable
logging.disable(logging.CRITICAL)

sys.path.insert(0, os.path.dirname(__file__))
from LB import LoadBalancer
from Server import Server

# ------------------------------------------------------------------ #
#  Configuration                                                       #
# ------------------------------------------------------------------ #
NUM_REQUESTS  = 40
QUEUE_TIMEOUT = 20        # seconds before a queued request is dropped
ALGO_NAMES    = {1: "Round Robin", 2: "Least Connections", 3: "Random", 4: "Weighted RR"}

LB_HOST       = "127.0.0.1"
LB_CLIENT_PORT = 20000
LB_SERVER_PORT = 10000

# Unequal capacities make algorithm differences clearly visible
SERVER_CONFIGS = [
    ("127.0.0.1", 9201, 1),   # weak  server: 1 slot
    ("127.0.0.1", 9202, 3),   # strong server: 3 slots
]
# Total capacity = 4 slots; 40 requests → 36 must queue


# ------------------------------------------------------------------ #
#  Request sender (one per thread)                                     #
# ------------------------------------------------------------------ #
def _send(i, results, errors):
    start = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(QUEUE_TIMEOUT + 5)
            sock.connect((LB_HOST, LB_CLIENT_PORT))
            sock.sendall(b"hello")
            resp = sock.recv(4096).decode()
            results[i] = ((time.time() - start) * 1000, resp)
    except Exception as e:
        errors[i] = str(e)


# ------------------------------------------------------------------ #
#  Run one algorithm                                                   #
# ------------------------------------------------------------------ #
def run_algorithm(algo_num):
    tag = f"[algo {algo_num}]"
    print(f"\n{'─'*58}")
    print(f"  {ALGO_NAMES[algo_num]}  (algorithm={algo_num})")
    print(f"{'─'*58}")

    # --- create LB (must be before any threads touch signal handlers)
    lb = LoadBalancer(algorithm=algo_num)
    lb.queue_timeout = QUEUE_TIMEOUT

    # --- create Server objects in the main thread
    #     (signal.signal() only works in the main thread)
    servers = [Server(h, p, capacity=c, load_profile="flat")
               for h, p, c in SERVER_CONFIGS]

    # --- start everything
    threading.Thread(target=lb.start, daemon=True).start()
    time.sleep(0.3)

    srv_threads = []
    for s in servers:
        t = threading.Thread(target=s.run, daemon=True)
        t.start()
        srv_threads.append(t)
        time.sleep(0.15)

    time.sleep(0.5)   # wait for server JOIN messages to reach LB
    caps = ", ".join(f"{h}:{p}(cap={c})" for h, p, c in SERVER_CONFIGS)
    print(f"{tag} servers: {caps}")
    print(f"{tag} firing {NUM_REQUESTS} concurrent requests ...")

    # --- fire all requests at once
    results, errors = {}, {}
    threads = [threading.Thread(target=_send, args=(i, results, errors))
               for i in range(NUM_REQUESTS)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=QUEUE_TIMEOUT + 10)
    wall = time.time() - t0

    # --- snapshot metrics before shutdown
    metrics = lb.get_metrics()

    # --- shut everything down
    lb.shutdown_event.set()
    for s in servers:
        s.shutdown_event.set()
    time.sleep(2.0)   # allow OS to release ports before next run

    # --- categorise results
    ok       = [(lat, r) for lat, r in results.values()
                if r not in ("NO AVAILABLE SERVER", "REQUEST TIMEOUT", "SERVER BUSY")]
    timeouts = [(lat, r) for lat, r in results.values() if r == "REQUEST TIMEOUT"]
    dropped  = [(lat, r) for lat, r in results.values()
                if r in ("NO AVAILABLE SERVER", "SERVER BUSY")]

    lats = sorted(lat for lat, _ in ok)
    n = len(lats)

    # per-server request counts from metrics
    req_counts = metrics.get("request_counts", {})

    return {
        "name":         ALGO_NAMES[algo_num],
        "completed":    len(ok),
        "timed_out":    len(timeouts),
        "hard_dropped": len(dropped),
        "net_errors":   len(errors),
        "wall_s":       round(wall, 1),
        "throughput":   round(len(ok) / wall, 2) if wall else 0,
        "avg_ms":       round(sum(lats) / n) if n else 0,
        "p50_ms":       round(lats[n // 2]) if n else 0,
        "p95_ms":       round(lats[min(int(n * 0.95), n - 1)]) if n else 0,
        "p99_ms":       round(lats[min(int(n * 0.99), n - 1)]) if n else 0,
        "total_queued": metrics.get("total_queued", 0),
        "q_timeouts":   metrics.get("queue_timeouts", 0),
        "fairness":     metrics.get("weighted_fairness"),
        "per_server":   req_counts,
    }


# ------------------------------------------------------------------ #
#  Main                                                                #
# ------------------------------------------------------------------ #
print("=" * 58)
print("  LB Algorithm Benchmark  —  High Concurrency")
print(f"  {NUM_REQUESTS} concurrent requests | "
      f"servers: cap=1 + cap=3 | flat 1s/req")
print("=" * 58)

rows = []
for algo in [1, 2, 3, 4]:
    rows.append(run_algorithm(algo))

# ------------------------------------------------------------------ #
#  Comparison table                                                    #
# ------------------------------------------------------------------ #
print(f"\n\n{'═'*78}")
print("  RESULTS COMPARISON")
print('═' * 78)

# Header
print(f"  {'Algorithm':<19} {'Done':>4} {'Queue':>5} {'T/O':>4} {'Drop':>4} "
      f"{'RPS':>5} {'avg':>5} {'p50':>5} {'p95':>5} {'p99':>5} {'Fair':>5}")
print(f"  {'─'*19} {'─'*4} {'─'*5} {'─'*4} {'─'*4} "
      f"{'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*5}")

for r in rows:
    fair = f"{r['fairness']:.3f}" if r['fairness'] is not None else "  N/A"
    print(f"  {r['name']:<19} {r['completed']:>4} {r['total_queued']:>5} "
          f"{r['timed_out']:>4} {r['hard_dropped']:>4} "
          f"{r['throughput']:>5.2f} "
          f"{r['avg_ms']:>5} {r['p50_ms']:>5} {r['p95_ms']:>5} {r['p99_ms']:>5} "
          f"{fair:>5}")

print('═' * 78)

# Per-server distribution table
print(f"\n  {'─'*58}")
print("  PER-SERVER REQUEST DISTRIBUTION")
print(f"  (ideal with cap=1+3: ~25% weak / ~75% strong)")
print(f"  {'─'*58}")
server_labels = [f"{h}:{p}(cap={c})" for h, p, c in SERVER_CONFIGS]
w = max(len(lb) for lb in server_labels) + 2
header2 = f"  {'Algorithm':<19}"
for lbl in server_labels:
    header2 += f"  {lbl:>{w}}"
print(header2)
print(f"  {'─'*19}" + ("  " + "─" * w) * len(server_labels))
for r in rows:
    line = f"  {r['name']:<19}"
    total = sum(r['per_server'].values()) or 1
    for h, p, _ in SERVER_CONFIGS:
        key = f"{h}:{p}"
        cnt = r['per_server'].get(key, 0)
        pct = cnt / total * 100
        line += f"  {cnt:>{w-5}}({pct:4.1f}%)"
    print(line)
print(f"  {'─'*58}")

print("""
  Column guide:
    Done  — requests that got a real response from a backend
    Queue — requests that had to wait in the LB queue
    T/O   — requests dropped after waiting > queue_timeout
    Drop  — requests immediately rejected (queue full / no server)
    RPS   — successful requests per second
    avg/p50/p95/p99 — latency in ms
    Fair  — Jain's weighted fairness index (1.000 = perfect)
""")
