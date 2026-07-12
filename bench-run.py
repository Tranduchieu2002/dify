#!/usr/bin/env python3
"""
Dify workflow benchmark — fires concurrent runs and reports throughput,
latency, success rate, and Redis queue depth.

Usage:
    python3 bench-run.py                      # default: fast workflow, 10 runs, 5 concurrent
    python3 bench-run.py --workflow code      # test sandbox code execution
    python3 bench-run.py -n 30 -c 10         # 30 runs, 10 concurrent
    python3 bench-run.py --workflow all       # run both suites back to back
"""
import argparse
import concurrent.futures
import json
import os
import socket
import statistics
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

# Override with env vars so the script works both locally and inside Docker.
#   DIFY_BASE_URL   - default http://localhost:5001
#   REDIS_HOST      - default localhost
#   REDIS_PORT      - default 6380  (Docker middleware exposes 6380 on host)
#   REDIS_PASSWORD  - default difyai123456
#   REDIS_DB        - default 0 (queue keys live in DB 0)
BASE_URL     = os.environ.get("DIFY_BASE_URL", "http://localhost:5001").rstrip("/") + "/v1"
_REDIS_HOST  = os.environ.get("REDIS_HOST", "localhost")
_REDIS_PORT  = int(os.environ.get("REDIS_PORT", "6380"))
_REDIS_PASS  = os.environ.get("REDIS_PASSWORD", "difyai123456")

WORKFLOWS = {
    "fast": {
        "name": "bench-fast-passthrough",
        "api_key": "app-IlTmZql4dcc3AdrU3ekF8Zyn",
        "inputs": {"query": "The quick brown fox jumps over the lazy dog"},
    },
    "code": {
        "name": "bench-code-execution",
        "api_key": "app-mxJXP0EwADAaBdjSqtKAfsZP",
        "inputs": {"query": "hello world benchmark scaling test dify worker queue"},
    },
    "heavy": {
        "name": "bench-heavy-wait",
        "api_key": "app-FNE94LP4IcPc9stvN8ywD25D",
        # wait_ms: simulated I/O wait per step (500ms step1 + 200ms step2 + 100ms step3 = ~800ms minimum)
        "inputs": {"query": "benchmark heavy workflow with multiple steps and waits", "wait_ms": "500"},
    },
}


# ── Redis helpers (raw RESP — no docker socket needed) ───────────────────────

def _resp_send(s: socket.socket, *args: str) -> None:
    cmd = f"*{len(args)}\r\n"
    for a in args:
        cmd += f"${len(a)}\r\n{a}\r\n"
    s.sendall(cmd.encode())


def _resp_read(s: socket.socket) -> str:
    buf = b""
    while not buf.endswith(b"\r\n"):
        buf += s.recv(256)
    return buf.decode().strip()


def redis_llen(queue: str) -> int:
    try:
        with socket.create_connection((_REDIS_HOST, _REDIS_PORT), timeout=3) as s:
            if _REDIS_PASS:
                _resp_send(s, "AUTH", _REDIS_PASS)
                _resp_read(s)
            _resp_send(s, "LLEN", queue)
            resp = _resp_read(s)
            return int(resp.lstrip(":"))
    except Exception:
        return -1


def redis_queue_depths() -> dict[str, int]:
    queues = ["workflow", "workflow_based_app_execution", "celery"]
    return {q: redis_llen(q) for q in queues}


# ── Single run ───────────────────────────────────────────────────────────────

@dataclass
class RunResult:
    idx: int
    duration_ms: float = 0.0
    status: str = "pending"
    run_id: str = ""
    error: str = ""


def run_once(idx: int, api_key: str, inputs: dict) -> RunResult:
    result = RunResult(idx=idx)
    t0 = time.monotonic()

    payload = json.dumps({
        "inputs": inputs,
        "response_mode": "blocking",
        "user": f"bench-user-{idx}",
    }).encode()

    req = urllib.request.Request(
        f"{BASE_URL}/workflows/run",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = json.loads(resp.read())
            result.duration_ms = (time.monotonic() - t0) * 1000
            result.run_id = body.get("workflow_run_id", "")
            wf_status = body.get("data", {}).get("status", "unknown")
            result.status = "ok" if wf_status in ("succeeded", "completed") else f"wf:{wf_status}"
    except urllib.error.HTTPError as e:
        result.duration_ms = (time.monotonic() - t0) * 1000
        result.status = f"http:{e.code}"
        result.error = e.read().decode()[:120]
    except Exception as exc:
        result.duration_ms = (time.monotonic() - t0) * 1000
        result.status = "error"
        result.error = str(exc)[:120]

    return result


# ── Queue monitor (background thread) ───────────────────────────────────────

class QueueMonitor(threading.Thread):
    def __init__(self, interval: float = 1.0):
        super().__init__(daemon=True)
        self.interval = interval
        self.samples: list[dict] = []
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            self.samples.append({"ts": time.monotonic(), **redis_queue_depths()})
            time.sleep(self.interval)

    def stop(self):
        self._stop.set()

    def peak(self, queue: str) -> int:
        return max((s.get(queue, 0) for s in self.samples), default=0)


# ── Benchmark suite ──────────────────────────────────────────────────────────

def run_suite(workflow_key: str, n: int, concurrency: int) -> None:
    cfg = WORKFLOWS[workflow_key]
    print(f"\n{'='*60}")
    print(f"  Workflow : {cfg['name']}")
    print(f"  Runs     : {n}  |  Concurrency : {concurrency}")
    print(f"  API key  : {cfg['api_key'][:20]}...")
    print(f"{'='*60}")

    # Pre-check: confirm API is up
    _health_url = BASE_URL.replace("/v1", "") + "/health"
    try:
        with urllib.request.urlopen(_health_url, timeout=5) as r:
            health = json.loads(r.read())
            print(f"  API health: {health.get('status')} v{health.get('version')}")
    except Exception as e:
        print(f"  WARNING: API health check failed: {e}")

    # Initial queue depths
    print(f"  Queue depth before: {redis_queue_depths()}")

    monitor = QueueMonitor(interval=0.5)
    monitor.start()

    wall_start = time.monotonic()
    results: list[RunResult] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(run_once, i, cfg["api_key"], cfg["inputs"]): i for i in range(n)}
        done_count = 0
        for fut in concurrent.futures.as_completed(futures):
            res = fut.result()
            results.append(res)
            done_count += 1
            icon = "✓" if res.status == "ok" else "✗"
            print(f"  [{done_count:>3}/{n}] {icon} run#{res.idx:<3} "
                  f"{res.duration_ms:>7.0f}ms  {res.status}"
                  + (f"  ERR: {res.error}" if res.error else ""))

    wall_ms = (time.monotonic() - wall_start) * 1000
    monitor.stop()

    # ── Stats ──
    ok = [r for r in results if r.status == "ok"]
    fail = [r for r in results if r.status != "ok"]
    durations = [r.duration_ms for r in ok]

    print(f"\n  ── Results ──────────────────────────────────")
    print(f"  Total wall time : {wall_ms/1000:.2f}s")
    print(f"  Throughput      : {n / (wall_ms/1000):.2f} runs/s")
    print(f"  Success         : {len(ok)}/{n}  ({100*len(ok)/n:.0f}%)")
    print(f"  Failures        : {len(fail)}")

    if durations:
        print(f"\n  ── Latency (successful runs) ─────────────")
        print(f"  Min  : {min(durations):.0f}ms")
        print(f"  P50  : {statistics.median(durations):.0f}ms")
        print(f"  P95  : {sorted(durations)[int(0.95*len(durations))-1]:.0f}ms")
        print(f"  Max  : {max(durations):.0f}ms")
        print(f"  Avg  : {statistics.mean(durations):.0f}ms")

    print(f"\n  ── Queue peaks (during run) ─────────────────")
    for q in ["workflow", "workflow_based_app_execution", "celery"]:
        print(f"  {q:<32} peak={monitor.peak(q)}")

    if fail:
        print(f"\n  ── Failures ──────────────────────────────────")
        seen = {}
        for r in fail:
            key = r.status
            if key not in seen:
                seen[key] = r
        for k, r in seen.items():
            print(f"  run#{r.idx}: {r.status}  {r.error}")


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description="Dify workflow benchmark")
    p.add_argument("--workflow", "-w", default="fast",
                   choices=["fast", "code", "heavy", "all"],
                   help="Which workflow to benchmark (default: fast)")
    p.add_argument("-n", "--runs", type=int, default=10,
                   help="Total number of workflow runs (default: 10)")
    p.add_argument("-c", "--concurrency", type=int, default=5,
                   help="Max concurrent runs (default: 5)")
    args = p.parse_args()

    targets = ["fast", "code", "heavy"] if args.workflow == "all" else [args.workflow]
    for wf in targets:
        run_suite(wf, args.runs, args.concurrency)

    print("\nDone. Check Flower at http://localhost:5555 for worker-level stats.")


if __name__ == "__main__":
    main()
