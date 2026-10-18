"""
Fault-tolerance test: the pipeline survives losing a Kafka broker.

  1. Places orders while all 3 brokers run.
  2. Stops one broker (docker stop cdc-kafka-2).
  3. Keeps placing orders: they must still reach Redis, none lost.
  4. Restarts the broker and checks every partition is fully replicated again.

Needs the Docker stack running and the docker CLI on PATH. Standard library only:
    python tests/test_failover.py
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8080"
BROKER = "cdc-kafka-2"


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def place_orders(n):
    ids = []
    for i in range(n):
        st, o = call("POST", "/api/write/orders", {"customer_id": 1 + i % 200, "payment_method": "upi",
                                                   "items": [{"product_id": 1 + i % 24, "quantity": 1}]})
        assert st == 201, o
        ids.append(o["order_id"])
    return ids


def wait_in_redis(ids, timeout=60):
    t0 = time.time()
    missing = set(ids)
    while missing and time.time() - t0 < timeout:
        missing = {i for i in missing if call("GET", f"/api/read/orders/{i}")[0] != 200}
        if missing:
            time.sleep(0.3)
    return missing, time.time() - t0


def under_replicated():
    out = subprocess.run(["docker", "exec", "cdc-kafka-1", "kafka-topics", "--bootstrap-server", "localhost:29092",
                          "--describe", "--under-replicated-partitions"], capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if line.strip().startswith("Topic:") and "Partition:" in line]


def step(msg):
    print(f"\n== {msg}")


def main():
    step("1. All 3 brokers up: placing 10 orders")
    ids = place_orders(10)
    missing, secs = wait_in_redis(ids)
    assert not missing, f"lost before failure: {missing}"
    print(f"   10/10 orders in Redis after {secs:.1f}s")

    step(f"2. Stopping broker {BROKER}")
    subprocess.run(["docker", "stop", BROKER], check=True, capture_output=True)
    time.sleep(3)
    print(f"   under-replicated partitions now: {len(under_replicated())} (expected > 0, data still has 2 copies)")

    step("3. One broker down: placing 20 more orders")
    ids2 = place_orders(20)
    missing, secs = wait_in_redis(ids2, timeout=90)
    assert not missing, f"orders lost while a broker was down: {sorted(missing)}"
    print(f"   20/20 orders reached Redis after {secs:.1f}s with only 2 brokers")

    step(f"4. Restarting {BROKER}")
    subprocess.run(["docker", "start", BROKER], check=True, capture_output=True)
    t0 = time.time()
    while time.time() - t0 < 120:
        if not under_replicated():
            break
        time.sleep(3)
    left = under_replicated()
    assert not left, f"{len(left)} partitions still under-replicated"
    print(f"   all partitions fully replicated again after {time.time() - t0:.0f}s")

    st, s = call("GET", "/api/read/stats")
    print(f"\nPASS: 30/30 orders survived a broker failure. Read model has {s['orders']} orders.")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"\nFAIL: {e}")
        subprocess.run(["docker", "start", BROKER], capture_output=True)
        sys.exit(1)
