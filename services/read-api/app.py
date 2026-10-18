"""
READ API  ("Read API Service" actor)

Serves every customer-facing read from the Redis read model. It has no
Postgres driver at all, so reads can never put load on the OLTP database.
The documents it returns were pre-joined (denormalized) by the CDC consumer,
so a full order lookup is a single Redis GET.

Endpoints (all under /api/read):
  GET /orders?status=&limit=&offset=   recent orders (optionally by status)
  GET /orders/<id>                     full order document
  GET /customers?limit=&offset=        customer summaries (order count, total spent)
  GET /customers/<id>                  customer + their recent orders
  GET /products                        product catalogue with category + live stock
  GET /products/<id>
  GET /stats                           dashboard KPIs + CDC pipeline telemetry
  GET /events?limit=                   latest CDC events applied by the consumer
  GET /consistency-snapshot            every order's status + total, for the consistency check
"""

import json
import os
import time

import redis
from flask import Flask, jsonify, request
from flask_cors import CORS

STATUSES = ("pending", "processing", "shipped", "delivered")

app = Flask(__name__)
CORS(app)

r = redis.Redis(
    host=os.environ.get("REDIS_HOST", "localhost"),
    port=int(os.environ.get("REDIS_PORT", "6379")),
    decode_responses=True,
)

# Sums every value of a hash inside Redis (no round trip per order).
SUM_HASH = r.register_script(
    "local s = 0 for _, v in ipairs(redis.call('HVALS', KEYS[1])) do s = s + tonumber(v) end return tostring(s)"
)


def page_args(default_limit=25, max_limit=200):
    limit = max(1, min(int(request.args.get("limit", default_limit)), max_limit))
    offset = max(0, int(request.args.get("offset", 0)))
    return limit, offset


def mget_views(prefix, ids):
    if not ids:
        return []
    return [json.loads(v) for v in r.mget([f"{prefix}:{i}" for i in ids]) if v]


def redis_json(payload, status=200):
    resp = jsonify(payload)
    resp.status_code = status
    resp.headers["X-Read-Source"] = "redis"
    return resp


@app.get("/api/read/orders")
def list_orders():
    limit, offset = page_args()
    status = request.args.get("status")
    key = f"orders:status:{status}" if status in STATUSES else "orders:recent"
    ids = r.zrevrange(key, offset, offset + limit - 1)
    return redis_json({"total": r.zcard(key), "orders": mget_views("view:order", ids)})


@app.get("/api/read/orders/<int:order_id>")
def get_order(order_id):
    t0 = time.perf_counter()
    raw = r.get(f"view:order:{order_id}")
    db_ms = (time.perf_counter() - t0) * 1000
    if raw is None:
        return redis_json({"error": "not found (yet) in read model"}, 404)
    resp = redis_json(json.loads(raw))
    resp.headers["Server-Timing"] = f"db;dur={db_ms:.3f}"   # time spent in the data store only
    return resp


@app.get("/api/read/customers")
def list_customers():
    limit, offset = page_args(50, 500)
    ids = r.zrange("customers:all", offset, offset + limit - 1)
    return redis_json({"total": r.zcard("customers:all"), "customers": mget_views("view:customer", ids)})


@app.get("/api/read/customers/<int:customer_id>")
def get_customer(customer_id):
    raw = r.get(f"view:customer:{customer_id}")
    if raw is None:
        return redis_json({"error": "not found"}, 404)
    customer = json.loads(raw)
    customer["recent_orders"] = sorted(
        mget_views("view:order", customer.get("order_ids", [])),
        key=lambda o: o["order_date"], reverse=True,
    )
    return redis_json(customer)


@app.get("/api/read/products")
def list_products():
    ids = r.zrange("products:all", 0, -1)
    return redis_json({"total": len(ids), "products": mget_views("view:product", ids)})


@app.get("/api/read/products/<int:product_id>")
def get_product(product_id):
    raw = r.get(f"view:product:{product_id}")
    if raw is None:
        return redis_json({"error": "not found"}, 404)
    return redis_json(json.loads(raw))


@app.get("/api/read/stats")
def stats():
    p = r.pipeline(transaction=False)
    p.zcard("orders:recent")
    p.zcard("customers:all")
    p.zcard("products:all")
    for s in STATUSES:
        p.zcard(f"orders:status:{s}")
    p.hgetall("cdc:stats")
    p.dbsize()
    res = p.execute()
    cdc = res[3 + len(STATUSES)]
    return redis_json({
        "orders": res[0],
        "customers": res[1],
        "products": res[2],
        "by_status": dict(zip(STATUSES, res[3:3 + len(STATUSES)])),
        "revenue": round(float(SUM_HASH(keys=["orders:totals"])), 2),
        "cdc": {k: int(v) for k, v in cdc.items()},
        "redis_keys": res[-1],
    })


@app.get("/api/read/events")
def events():
    limit = max(1, min(int(request.args.get("limit", 30)), 200))
    return redis_json([json.loads(e) for e in r.lrange("cdc:events", 0, limit - 1)])


@app.get("/api/read/consistency-snapshot")
def consistency_snapshot():
    """Compact {order_id: status, total} map, compared against Postgres by the dashboard."""
    p = r.pipeline(transaction=False)
    p.hgetall("orders:totals")
    for s in STATUSES:
        p.zrange(f"orders:status:{s}", 0, -1)
    totals, *by_status = p.execute()
    status_of = {oid: s for s, ids in zip(STATUSES, by_status) for oid in ids}
    return redis_json([
        {"order_id": int(oid), "status": status_of.get(oid), "total_amount": round(float(t), 2)}
        for oid, t in totals.items()
    ])


@app.get("/api/read/health")
def health():
    r.ping()
    return redis_json({"ok": True, "store": "redis"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5002")), debug=False, threaded=True)
