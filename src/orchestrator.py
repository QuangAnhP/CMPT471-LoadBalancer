"""
orchestrator.py — Experiment runner for the load balancer.

Spawns a load balancer and N backend servers as subprocesses, then runs M
concurrent clients (in threads, in-process) and aggregates metrics.

Each server's 'processing delay' controls the simulated processing time per request:
	- constant: always 1.0s
	- variable: Gaussian(mean=1.0s, std=0.3s), clamped to [0.05, 5.0]
	- bimodal: 80% fast (0.2s), 20% slow (3.0s)

Usage:
  python orchestrator.py [options]

Examples:
    # Round Robin, 3 servers, 5 clients, 10 requests each, constant processing time:
    python orchestrator.py --algorithm 1 --servers 3 --clients 5 --requests 10

    # Weighted Round Robin with bimodal processing servers, kill one mid-experiment:
    python orchestrator.py --algorithm 4 --servers 4 --clients 8 --requests 20 \\
            --processing-delay bimodal --kill-server

	# Compare all algorithms (runs 4 experiments sequentially):
	python orchestrator.py --compare-all --servers 3 --clients 5 --requests 10
"""
import sys
import os
import time
import json
import threading
import subprocess
import argparse
import statistics
import csv

# Allow importing Client from the same directory
sys.path.insert(0, os.path.dirname(__file__))
from Client import Client

ALGO_NAMES = {1: "Round Robin", 2: "Least Connections", 3: "Random", 4: "Weighted RR"}
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
LB_PATH = os.path.join(SRC_DIR, "LB.py")
SV_PATH = os.path.join(SRC_DIR, "Server.py")


# ------------------------------------------------------------------ #
#  Client thread worker                                               #
# ------------------------------------------------------------------ #

def _run_client(client_id, lb_host, lb_port, num_requests, interval, message, results):
    c = Client(lb_host, lb_port, num_requests=num_requests, interval=interval,
               message=message, persistent_connection=False)
    c.run()
    results[client_id] = (c.latencies, c.responses)


# ------------------------------------------------------------------ #
#  Single experiment                                                  #
# ------------------------------------------------------------------ #

def run_experiment(algorithm, num_servers, num_clients, num_requests,
                   interval, processing_delay, kill_server, output_dir):
    """
    Run one experiment. Returns a dict of aggregate metrics.
    """
    algo_name = ALGO_NAMES.get(algorithm, str(algorithm))
    metrics_file = os.path.join(output_dir, f"lb_metrics_algo{algorithm}.json")

    print(f"\n{'='*60}")
    print(f"  Algorithm : {algorithm} — {algo_name}")
    print(f"  Servers   : {num_servers}  |  Clients: {num_clients}  |  Req/client: {num_requests}")
    print(f"  Processing Delay Profile: {processing_delay}  |  Kill server: {kill_server}")
    print(f"{'='*60}\n")

    # -- Start load balancer ----------------------------------------
    lb_proc = subprocess.Popen(
        [sys.executable, LB_PATH, str(algorithm), metrics_file],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    time.sleep(0.6)

    # -- Start backend servers with varying capacities ---------------
    # Capacities: 2, 4, 6, ... so servers have different weights
    # Each server's processing_delay controls simulated processing time per request
    server_procs = []
    base_port = 9300
    for i in range(num_servers):
        host = f"127.{10 + i}.0.2"
        port = base_port + i
        capacity = 5 if i == 0 else (i + 1) * 5
        proc = subprocess.Popen(
            [sys.executable, SV_PATH, host, str(port), str(capacity),
             f"--processing-delay={processing_delay}"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        server_procs.append((proc, host, port, capacity))
        print(f"  Server {i+1}: {host}:{port}  capacity={capacity}  processing_delay={processing_delay}")
    time.sleep(0.6)

    # -- Launch client threads ---------------------------------------
    results = {}
    threads = []
    for i in range(num_clients):
        t = threading.Thread(
            target=_run_client,
            args=(i, "127.0.0.1", 20000, num_requests, interval, "ping", results),
            daemon=True
        )
        threads.append(t)

    experiment_start = time.time()
    for t in threads:
        t.start()

    # -- Optionally kill a server halfway through --------------------
    if kill_server and num_servers > 1:
        kill_delay = (num_requests * interval) / 2
        time.sleep(kill_delay)
        victim_proc, victim_host, victim_port, _ = server_procs[0]
        victim_proc.terminate()
        print(f"\n  [MID-EXPERIMENT] Killed server {victim_host}:{victim_port}\n")

    for t in threads:
        t.join()
    elapsed = time.time() - experiment_start

    # -- Aggregate client metrics ------------------------------------
    # results[cid] = (latencies, responses) — see _run_client
    all_latencies = []
    errors = 0
    for lats, resps in results.values():
        for l, r in zip(lats, resps):
            if l is None or not r.startswith("Processed by"):
                errors += 1
            else:
                all_latencies.append(l)

    s = sorted(all_latencies)
    n = len(s)
    avg = sum(s) / n if n else 0
    p50 = s[n // 2] if n else 0
    p95 = s[min(int(n * 0.95), n - 1)] if n else 0
    diffs = [abs(all_latencies[i + 1] - all_latencies[i]) for i in range(len(all_latencies) - 1)]
    jitter = statistics.stdev(diffs) if len(diffs) > 1 else 0.0
    throughput = n / elapsed if elapsed > 0 else 0

    print(f"\n--- Client Results ---")
    print(f"  Total    : {num_clients * num_requests} req  |  OK: {n}  |  Errors: {errors}")
    print(f"  Latency  : Avg={avg:.1f}ms  P50={p50:.1f}ms  P95={p95:.1f}ms")
    print(f"  Jitter   : {jitter:.1f} ms  |  Throughput: {throughput:.2f} req/s")

    # -- Read LB metrics (wait for background writer) ---------------
    time.sleep(2.5)
    lb_data = None
    if os.path.exists(metrics_file):
        try:
            with open(metrics_file) as f:
                lb_data = json.load(f)
        except Exception:
            pass

    if lb_data:
        print(f"\n--- Server Distribution ---")
        req_counts = lb_data.get("request_counts", {})
        total_req = sum(req_counts.values()) or 1
        for label, count in req_counts.items():
            avg_util = lb_data.get("avg_utilization", {}).get(label, 0)
            print(f"  {label}: {count} req ({100*count/total_req:.1f}%)  "
                  f"avg utilization {avg_util*100:.1f}%")
        fairness = lb_data.get("weighted_fairness")
        if fairness is not None:
            print(f"  Weighted Fairness (Jain's index): {fairness:.4f}")
        print(f"  LB throughput (last 10s window): {lb_data.get('throughput_rps', 0):.3f} req/s")

    # -- Write per-experiment summary CSV ---------------------------
    summary = {
        "algorithm": algorithm,
        "algo_name": algo_name,
        "servers": num_servers,
        "clients": num_clients,
        "requests_per_client": num_requests,
        "processing_delay": processing_delay,
        "kill_server": kill_server,
        "total_ok": n,
        "total_errors": errors,
        "duration_s": round(elapsed, 3),
        "avg_ms": round(avg, 2),
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "jitter_ms": round(jitter, 2),
        "throughput_rps": round(throughput, 3),
        "weighted_fairness": lb_data.get("weighted_fairness") if lb_data else None,
    }

    # -- Cleanup -----------------------------------------------------
    lb_proc.terminate()
    for proc, _, _, _ in server_procs:
        proc.terminate()
    lb_proc.wait()
    for proc, _, _, _ in server_procs:
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Give OS time to release ports before the next experiment
    time.sleep(1.5)
    return summary


# ------------------------------------------------------------------ #
#  Main                                                               #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Load Balancer Experiment Orchestrator")
    parser.add_argument("--algorithm", type=int, default=1, choices=[1, 2, 3, 4],
                        help="LB algorithm (1=RR, 2=LC, 3=Random, 4=WRR)")
    parser.add_argument("--servers", type=int, default=3,
                        help="Number of backend servers")
    parser.add_argument("--clients", type=int, default=5,
                        help="Number of concurrent clients")
    parser.add_argument("--requests", type=int, default=10,
                        help="Requests per client")
    parser.add_argument("--interval", type=float, default=0.05,
                        help="Seconds between client requests (default: 0.05)")
    parser.add_argument("--processing-delay", default="constant", choices=["constant", "variable", "bimodal"],
                        help="Adjustable server processing delay profile")
    parser.add_argument("--kill-server", action="store_true",
                        help="Kill one server mid-experiment to test failover")
    parser.add_argument("--output-dir", default="results",
                        help="Directory for output files (default: results/)")
    parser.add_argument("--compare-all", action="store_true",
                        help="Run all 4 algorithms and print a comparison table")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if args.compare_all:
        summaries = []
        for algo in [1, 2, 3, 4]:
            s = run_experiment(
                algorithm=algo,
                num_servers=args.servers,
                num_clients=args.clients,
                num_requests=args.requests,
                interval=args.interval,
                processing_delay=args.processing_delay,
                kill_server=args.kill_server,
                output_dir=args.output_dir,
            )
            summaries.append(s)
        _print_comparison_table(summaries)
        _write_comparison_csv(summaries, os.path.join(args.output_dir, "comparison.csv"))
    else:
        run_experiment(
            algorithm=args.algorithm,
            num_servers=args.servers,
            num_clients=args.clients,
            num_requests=args.requests,
            interval=args.interval,
            processing_delay=args.processing_delay,
            kill_server=args.kill_server,
            output_dir=args.output_dir,
        )


def _print_comparison_table(summaries):
    print(f"\n{'='*70}")
    print("  ALGORITHM COMPARISON")
    print(f"{'='*70}")
    header = f"  {'Algorithm':<22} {'OK/Total':>10} {'Avg ms':>8} {'P95 ms':>8} {'Fair':>6}"
    print(header)
    print(f"  {'-'*65}")
    for s in summaries:
        total = s["clients"] * s["requests_per_client"] if "clients" in s else "?"
        fairness = f"{s['weighted_fairness']:.3f}" if s.get("weighted_fairness") is not None else "  N/A"
        print(f"  {s['algo_name']:<22} {s['total_ok']:>5}/{total:<4} "
              f"{s['avg_ms']:>8.1f} {s['p95_ms']:>8.1f} {fairness:>6}")
    print(f"{'='*70}\n")


def _write_comparison_csv(summaries, filepath):
    if not summaries:
        return
    fields = list(summaries[0].keys())
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summaries)
    print(f"Comparison table written to {filepath}")


if __name__ == "__main__":
    main()
