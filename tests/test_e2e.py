"""
End-to-end tests for the running stack (docker compose up -d --build first).

Every test writes through the Write API (PostgreSQL) and then waits for the
change to appear through the Read API (Redis), so they exercise the whole
pipeline: Postgres -> WAL -> Debezium -> Kafka -> consumer -> Redis.

Uses only the Python standard library:
    python tests/test_e2e.py            # or: python -m unittest -v tests/test_e2e.py
    BASE_URL=http://localhost:30080 python tests/test_e2e.py   # against Kubernetes
"""

import json
import os
import time
import unittest
import urllib.error
import urllib.request

BASE = os.environ.get("BASE_URL", "http://localhost:8080")
TIMEOUT_S = 15


def call(method, path, body=None):
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def eventually(check, timeout=TIMEOUT_S):
    """Poll until check() is truthy; returns elapsed ms, or fails the test."""
    t0 = time.perf_counter()
    while time.perf_counter() - t0 < timeout:
        try:
            if check():
                return (time.perf_counter() - t0) * 1000
        except (KeyError, TypeError, IndexError):
            pass
        time.sleep(0.03)
    raise AssertionError(f"read model did not converge within {timeout}s")


def read_order(oid):
    return call("GET", f"/api/read/orders/{oid}")


class Pipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        st, _ = call("GET", "/api/write/health")
        st2, _ = call("GET", "/api/read/health")
        if st != 200 or st2 != 200:
            raise unittest.SkipTest(f"stack not reachable at {BASE}")
        eventually(lambda: call("GET", "/api/read/stats")[1]["orders"] > 0, timeout=120)

    def place(self, items, customer_id=2, method="upi"):
        st, o = call("POST", "/api/write/orders",
                     {"customer_id": customer_id, "items": items, "payment_method": method})
        self.assertEqual(st, 201, o)
        return o

    # ------------------------------------------------------------ happy paths
    def test_create_order_is_projected_into_redis(self):
        price = call("GET", "/api/read/products/5")[1]["price"]
        o = self.place([{"product_id": 5, "quantity": 2}])
        self.assertEqual(o["total_amount"], 2 * price)          # trigger-computed total
        eventually(lambda: read_order(o["order_id"])[1]["payment"]["status"] == "completed")
        st, view = read_order(o["order_id"])
        self.assertEqual(view["items"][0]["quantity"], 2)
        self.assertEqual(view["customer"]["customer_id"], 2)
        self.assertEqual(view["history"][0]["new_status"], "pending")   # history trigger

    def test_stock_is_reduced_and_replicated(self):
        before = call("GET", "/api/read/products/7")[1]["stock"]
        self.place([{"product_id": 7, "quantity": 3}])
        eventually(lambda: call("GET", "/api/read/products/7")[1]["stock"] == before - 3)

    def test_status_change_writes_history(self):
        oid = self.place([{"product_id": 1, "quantity": 1}])["order_id"]
        st, _ = call("PATCH", f"/api/write/orders/{oid}/status", {"status": "shipped"})
        self.assertEqual(st, 200)
        eventually(lambda: read_order(oid)[1]["history"][-1]["new_status"] == "shipped")
        self.assertEqual(read_order(oid)[1]["status"], "shipped")

    def test_cod_is_settled_on_delivery(self):
        oid = self.place([{"product_id": 2, "quantity": 1}], method="cod")["order_id"]
        eventually(lambda: read_order(oid)[1]["payment"]["status"] == "pending")
        call("PATCH", f"/api/write/orders/{oid}/status", {"status": "delivered"})
        eventually(lambda: read_order(oid)[1]["payment"]["status"] == "completed")

    def test_removing_an_item_recomputes_total(self):
        o = self.place([{"product_id": 12, "quantity": 1}, {"product_id": 13, "quantity": 2}])
        st, r = call("DELETE", f"/api/write/orders/{o['order_id']}/items/12")
        self.assertEqual(st, 200)
        eventually(lambda: (lambda v: len(v["items"]) == 1 and v["total_amount"] == r["total_amount"])(read_order(o["order_id"])[1]))

    def test_delete_cascades_and_restocks(self):
        before = call("GET", "/api/read/products/9")[1]["stock"]
        oid = self.place([{"product_id": 9, "quantity": 4}])["order_id"]
        eventually(lambda: read_order(oid)[0] == 200)
        self.assertEqual(call("DELETE", f"/api/write/orders/{oid}")[0], 200)
        eventually(lambda: read_order(oid)[0] == 404)
        eventually(lambda: call("GET", "/api/read/products/9")[1]["stock"] == before)

    def test_customer_change_updates_denormalized_orders(self):
        oid = self.place([{"product_id": 3, "quantity": 1}], customer_id=4)["order_id"]
        eventually(lambda: read_order(oid)[0] == 200)
        city = f"Testpur-{int(time.time())}"
        self.assertEqual(call("PATCH", "/api/write/customers/4", {"city": city})[0], 200)
        eventually(lambda: read_order(oid)[1]["customer"]["city"] == city)

    # -------------------------------------------- constraints & rollbacks
    def test_oversell_is_rolled_back(self):
        before = call("GET", "/api/read/orders?limit=1")[1]["total"]
        st, body = call("POST", "/api/write/orders", {"customer_id": 1, "items": [{"product_id": 24, "quantity": 10**6}]})
        self.assertEqual(st, 409, body)
        time.sleep(0.5)
        self.assertEqual(call("GET", "/api/read/orders?limit=1")[1]["total"], before)

    def test_unknown_customer_rejected(self):
        st, _ = call("POST", "/api/write/orders", {"customer_id": 10**6, "items": [{"product_id": 1, "quantity": 1}]})
        self.assertEqual(st, 404)

    def test_check_constraint_rejects_negative_price(self):
        st, body = call("PATCH", "/api/write/products/1", {"price": -1})
        self.assertEqual(st, 400)
        self.assertIn("check constraint", body["detail"])

    def test_invalid_status_rejected(self):
        oid = call("GET", "/api/read/orders?limit=1")[1]["orders"][0]["order_id"]
        self.assertEqual(call("PATCH", f"/api/write/orders/{oid}/status", {"status": "lost"})[0], 400)

    # ------------------------------------------------- system properties
    def test_dual_write_crash_leaves_naive_cache_stale_but_cdc_correct(self):
        call("POST", "/api/write/demo/dual-write", {"order_id": 15, "status": "processing", "crash": False})
        st, r = call("POST", "/api/write/demo/dual-write", {"order_id": 15, "status": "shipped", "crash": True})
        self.assertEqual(r["postgres_status"], "shipped")
        self.assertEqual(r["naive_cache_status"], "processing")      # the dual-write bug
        eventually(lambda: read_order(15)[1]["status"] == "shipped")  # CDC converges

    def test_redis_converges_to_postgres(self):
        self.assertEqual(call("POST", "/api/write/simulate", {"orders": 10})[0], 200)

        def same():
            pg = {o["order_id"]: o for o in call("GET", "/api/write/direct/orders")[1]}
            rd = {o["order_id"]: o for o in call("GET", "/api/read/consistency-snapshot")[1]}
            return pg.keys() == rd.keys() and all(
                pg[k]["status"] == rd[k]["status"] and abs(pg[k]["total_amount"] - rd[k]["total_amount"]) < 1e-6
                for k in pg)
        eventually(same, timeout=30)

    def test_end_to_end_latency_under_one_second(self):
        oid = call("GET", "/api/read/orders?limit=1")[1]["orders"][0]["order_id"]
        status = "processing" if read_order(oid)[1]["status"] != "processing" else "shipped"
        call("PATCH", f"/api/write/orders/{oid}/status", {"status": status})
        ms = eventually(lambda: read_order(oid)[1]["status"] == status)
        print(f"\n    commit -> Redis: {ms:.0f} ms", end=" ")
        self.assertLess(ms, 1000)

    def test_read_api_serves_from_redis(self):
        st, _ = read_order(1)
        req = urllib.request.Request(BASE + "/api/read/orders/1")
        with urllib.request.urlopen(req) as r:
            self.assertEqual(r.headers["X-Read-Source"], "redis")


if __name__ == "__main__":
    unittest.main(verbosity=2)
