"""
Single-terminal stress test for the waiting queue.

Starts LB + 2 slow servers (capacity=2 each) in background threads,
then fires 30 concurrent requests. Only 4 requests can run at a time,
so the rest queue up and are served as slots free.

Run from the src/ directory:
    python test_queue.py
"""
import socket
import threading
import time
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from LB import LoadBalancer
from Server import Server


# ------------------------------------------------------------------ #
#  Configuration                                                       #
# ------------------------------------------------------------------ #
LB_HOST      = "127.0.0.1"
LB_PORT      = 20000
SERVER_HOSTS = [("127.0.0.1", 9101), ("127.0.0.1", 9102)]
CAPACITY     = 2      # slots per server  (4 total → forces queueing)
NUM_REQUESTS = 30     # concurrent requests to fire
QUEUE_TIMEOUT = 15    # seconds LB will wait before timing out a queued request


# ------------------------------------------------------------------ #
#  Start LB                                                            #
# ------------------------------------------------------------------ #
lb = LoadBalancer(algorithm=1)
lb.queue_timeout = QUEUE_TIMEOUT
threading.Thread(target=lb.start, daemon=True).start()
time.sleep(0.3)
print("[setup] Load balancer started")

# ------------------------------------------------------------------ #
#  Start servers                                                       #
# ------------------------------------------------------------------ #
for host, port in SERVER_HOSTS:
    s = Server(host, port, capacity=CAPACITY, load_profile="flat")
    threading.Thread(target=s.run, daemon=True).start()
    time.sleep(0.2)
    print(f"[setup] Server started on {host}:{port}  (capacity={CAPACITY}, ~1s per request)")

time.sleep(0.5)  # let servers register with LB

# ------------------------------------------------------------------ #
#  Fire concurrent requests                                            #
# ------------------------------------------------------------------ #
results = {}   # index -> (latency_ms, response)
errors  = {}

def send_request(i):
    start = time.time()
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(QUEUE_TIMEOUT + 5)
            sock.connect((LB_HOST, LB_PORT))
            sock.sendall(b"hello")
            resp = sock.recv(4096).decode()
            latency = (time.time() - start) * 1000
            results[i] = (latency, resp)
    except Exception as e:
        errors[i] = str(e)

print(f"\n[test] Firing {NUM_REQUESTS} concurrent requests "
      f"(server capacity={CAPACITY}×2={CAPACITY*2}, queue_timeout={QUEUE_TIMEOUT}s) ...\n")

threads = [threading.Thread(target=send_request, args=(i,)) for i in range(NUM_REQUESTS)]
t0 = time.time()
for t in threads:
    t.start()
for t in threads:
    t.join(timeout=QUEUE_TIMEOUT + 10)

total_elapsed = time.time() - t0

# ------------------------------------------------------------------ #
#  Print results                                                       #
# ------------------------------------------------------------------ #
print("-" * 60)
successes   = [(i, lat, resp) for i, (lat, resp) in results.items()
               if resp not in ("NO AVAILABLE SERVER", "REQUEST TIMEOUT", "SERVER BUSY")]
queued_resp = [(i, lat, resp) for i, (lat, resp) in results.items() if resp == "REQUEST TIMEOUT"]
dropped     = [(i, lat, resp) for i, (lat, resp) in results.items()
               if resp in ("NO AVAILABLE SERVER", "SERVER BUSY")]

print(f"Total requests : {NUM_REQUESTS}")
print(f"  Completed    : {len(successes)}")
print(f"  Timed out    : {len(queued_resp)}  (waited >{QUEUE_TIMEOUT}s in queue)")
print(f"  Hard-dropped : {len(dropped)}")
print(f"  Errors       : {len(errors)}")
print(f"Wall-clock time: {total_elapsed:.1f}s")

if successes:
    lats = sorted(lat for _, lat, _ in successes)
    n = len(lats)
    print(f"\nLatency of completed requests (ms):")
    print(f"  avg={sum(lats)/n:.0f}  "
          f"p50={lats[n//2]:.0f}  "
          f"p95={lats[min(int(n*0.95), n-1)]:.0f}  "
          f"p99={lats[min(int(n*0.99), n-1)]:.0f}")

print("\nLB queue metrics:", lb.get_metrics().get("total_queued"), "queued,",
      lb.get_metrics().get("queue_timeouts"), "timed out")
print("-" * 60)
