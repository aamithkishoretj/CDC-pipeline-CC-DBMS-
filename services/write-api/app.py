"""
WRITE API  ("Order Processing System" actor)

The single write path of the system: every create / update / delete goes to
PostgreSQL inside one ACID transaction, and NOTHING here writes to the Redis
read model. Redis is kept in sync by the CDC pipeline (Debezium reads the
WAL -> Kafka -> consumer). That is how the dual-write problem is avoided.

Endpoints (all under /api/write):
  POST   /orders                         create order (+items, +payment) in one transaction
  PATCH  /orders/<id>/status             change status (trigger writes the history row)
  DELETE /orders/<id>/items/<product_id> remove one line item (trigger recomputes total)
  DELETE /orders/<id>                    delete order (cascade) and restock its items
  POST   /customers                      create customer
  PATCH  /customers/<id>                 update customer details
  PATCH  /products/<id>                  update price / stock
  POST   /simulate                       generate random write traffic for demos

  GET    /direct/orders/<id>             "before" approach: read straight from Postgres with JOINs
  GET    /direct/orders                  compact list, used by the consistency checker
  GET    /direct/lookups                 customers + products for the order form

  POST   /demo/dual-write                the naive approach (PG + Redis), to show the failure
"""

import json
import os
import random
import time
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import psycopg2.errors
import psycopg2.extras
import psycopg2.pool
from flask import Flask, jsonify, request
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS

STATUSES = ("pending", "processing", "shipped", "delivered")
PAYMENT_METHODS = ("card", "upi", "netbanking", "cod")


class JSONProvider(DefaultJSONProvider):
    @staticmethod
    def default(o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, (datetime, date)):
            return o.isoformat(timespec="seconds") if isinstance(o, datetime) else o.isoformat()
        return DefaultJSONProvider.default(o)


app = Flask(__name__)
app.json = JSONProvider(app)
CORS(app)

PG_DSN = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=int(os.environ.get("PG_PORT", "5433")),
    user=os.environ.get("PG_USER", "app_writer"),
    password=os.environ.get("PG_PASSWORD", "writerpass"),
    dbname=os.environ.get("PG_DB", "shopdb"),
)
_pool = None


def pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 20, **PG_DSN)
    return _pool


@contextmanager
def transaction():
    """BEGIN ... COMMIT, or ROLLBACK on any exception (atomicity)."""
    conn = pool().getconn()
    try:
        with conn:  # psycopg2: commits on success, rolls back on exception
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                yield cur
    finally:
        pool().putconn(conn)


class ApiError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.message, self.status = message, status


@app.errorhandler(ApiError)
def handle_api_error(e):
    return jsonify({"error": e.message}), e.status


@app.errorhandler(psycopg2.errors.CheckViolation)
@app.errorhandler(psycopg2.errors.UniqueViolation)
@app.errorhandler(psycopg2.errors.ForeignKeyViolation)
@app.errorhandler(psycopg2.errors.NotNullViolation)
def handle_constraint(e):
    # Constraint violations are rejected by the DBMS itself, not by app code.
    return jsonify({"error": "constraint violation", "detail": e.pgerror.strip().splitlines()[0]}), 400


def body():
    return request.get_json(silent=True) or {}


def committed_at():
    return int(time.time() * 1000)


# ---------------------------------------------------------------- orders
def create_order_tx(cur, customer_id, items, method):
    if not items:
        raise ApiError("an order needs at least one item")
    qty = {}
    for it in items:
        pid, q = int(it["product_id"]), int(it["quantity"])
        if q <= 0:
            raise ApiError("quantity must be positive")
        qty[pid] = qty.get(pid, 0) + q

    cur.execute("SELECT 1 FROM customers WHERE customer_id = %s", (customer_id,))
    if cur.fetchone() is None:
        raise ApiError(f"customer {customer_id} does not exist", 404)

    # Concurrency control: lock the product rows (in a fixed order, so two
    # concurrent orders can never deadlock) before checking and reducing stock.
    cur.execute(
        "SELECT product_id, name, price, stock FROM products "
        "WHERE product_id = ANY(%s) ORDER BY product_id FOR UPDATE",
        (list(qty),),
    )
    products = {p["product_id"]: p for p in cur.fetchall()}
    for pid, q in qty.items():
        if pid not in products:
            raise ApiError(f"product {pid} does not exist", 404)
        if products[pid]["stock"] < q:
            raise ApiError(f"not enough stock for '{products[pid]['name']}' (have {products[pid]['stock']}, need {q})", 409)

    cur.execute(
        "INSERT INTO orders (customer_id, order_date, status, total_amount) "
        "VALUES (%s, NOW(), 'pending', 0) RETURNING order_id",
        (customer_id,),
    )
    order_id = cur.fetchone()["order_id"]

    for pid, q in qty.items():
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, pid, q, products[pid]["price"]),
        )
        cur.execute("UPDATE products SET stock = stock - %s WHERE product_id = %s", (q, pid))

    # total_amount was filled in by trg_order_items_total
    cur.execute("SELECT total_amount FROM orders WHERE order_id = %s", (order_id,))
    total = cur.fetchone()["total_amount"]

    paid = method != "cod"
    cur.execute(
        "INSERT INTO payments (order_id, method, amount, status, paid_at) "
        "VALUES (%s, %s, %s, %s, CASE WHEN %s THEN NOW() END)",
        (order_id, method, total, "completed" if paid else "pending", paid),
    )
    return order_id, total


@app.post("/api/write/orders")
def create_order():
    data = body()
    method = data.get("payment_method", "upi")
    if method not in PAYMENT_METHODS:
        raise ApiError(f"payment_method must be one of {PAYMENT_METHODS}")
    try:
        customer_id = int(data["customer_id"])
        items = data.get("items") or []
    except (KeyError, ValueError, TypeError):
        raise ApiError("customer_id and items[] are required")
    with transaction() as cur:
        order_id, total = create_order_tx(cur, customer_id, items, method)
    return jsonify({"order_id": order_id, "total_amount": total, "status": "pending",
                    "committed_at": committed_at()}), 201


@app.patch("/api/write/orders/<int:order_id>/status")
def update_status(order_id):
    status = body().get("status")
    if status not in STATUSES:
        raise ApiError(f"status must be one of {STATUSES}")
    with transaction() as cur:
        cur.execute(
            "UPDATE orders SET status = %s WHERE order_id = %s RETURNING order_id, status",
            (status, order_id),
        )
        row = cur.fetchone()
        if row is None:
            raise ApiError("order not found", 404)
        if status == "delivered":
            # Cash-on-delivery is collected on delivery: same transaction.
            cur.execute(
                "UPDATE payments SET status = 'completed', paid_at = NOW() "
                "WHERE order_id = %s AND status = 'pending'",
                (order_id,),
            )
    return jsonify({**row, "committed_at": committed_at()})


@app.delete("/api/write/orders/<int:order_id>/items/<int:product_id>")
def delete_item(order_id, product_id):
    with transaction() as cur:
        cur.execute(
            "DELETE FROM order_items WHERE order_id = %s AND product_id = %s RETURNING quantity",
            (order_id, product_id),
        )
        row = cur.fetchone()
        if row is None:
            raise ApiError("item not found", 404)
        cur.execute("UPDATE products SET stock = stock + %s WHERE product_id = %s", (row["quantity"], product_id))
        cur.execute("SELECT total_amount FROM orders WHERE order_id = %s", (order_id,))
        total = cur.fetchone()["total_amount"]
        cur.execute("UPDATE payments SET amount = %s WHERE order_id = %s", (total, order_id))
    return jsonify({"order_id": order_id, "removed_product_id": product_id,
                    "total_amount": total, "committed_at": committed_at()})


@app.delete("/api/write/orders/<int:order_id>")
def delete_order(order_id):
    with transaction() as cur:
        # Put the stock back, then delete; ON DELETE CASCADE removes items,
        # payments and history. Either all of it happens or none of it.
        cur.execute(
            "UPDATE products p SET stock = p.stock + oi.quantity "
            "FROM order_items oi WHERE oi.product_id = p.product_id AND oi.order_id = %s",
            (order_id,),
        )
        cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
        if cur.rowcount == 0:
            raise ApiError("order not found", 404)
    return jsonify({"deleted": order_id, "committed_at": committed_at()})


# ------------------------------------------------------ customers/products
@app.post("/api/write/customers")
def create_customer():
    d = body()
    missing = [f for f in ("name", "email", "address", "city") if not d.get(f)]
    if missing:
        raise ApiError(f"missing fields: {', '.join(missing)}")
    with transaction() as cur:
        cur.execute(
            "INSERT INTO customers (name, email, phone, address, city) VALUES (%s,%s,%s,%s,%s) "
            "RETURNING customer_id",
            (d["name"], d["email"], d.get("phone"), d["address"], d["city"]),
        )
        cid = cur.fetchone()["customer_id"]
    return jsonify({"customer_id": cid, "committed_at": committed_at()}), 201


@app.patch("/api/write/customers/<int:customer_id>")
def update_customer(customer_id):
    d = {k: v for k, v in body().items() if k in ("name", "email", "phone", "address", "city")}
    if not d:
        raise ApiError("nothing to update")
    sets = ", ".join(f"{k} = %s" for k in d)
    with transaction() as cur:
        cur.execute(f"UPDATE customers SET {sets} WHERE customer_id = %s", (*d.values(), customer_id))
        if cur.rowcount == 0:
            raise ApiError("customer not found", 404)
    return jsonify({"customer_id": customer_id, "updated": list(d), "committed_at": committed_at()})


@app.patch("/api/write/products/<int:product_id>")
def update_product(product_id):
    d = {k: v for k, v in body().items() if k in ("name", "price", "stock", "description")}
    if not d:
        raise ApiError("nothing to update")
    sets = ", ".join(f"{k} = %s" for k in d)
    with transaction() as cur:
        cur.execute(f"UPDATE products SET {sets} WHERE product_id = %s", (*d.values(), product_id))
        if cur.rowcount == 0:
            raise ApiError("product not found", 404)
    return jsonify({"product_id": product_id, "updated": list(d), "committed_at": committed_at()})


# ------------------------------------------------------------- simulator
@app.post("/api/write/simulate")
def simulate():
    n = max(1, min(int(body().get("orders", 10)), 100))
    created, advanced, failed = 0, 0, 0
    with transaction() as cur:
        cur.execute("SELECT customer_id FROM customers")
        customers = [r["customer_id"] for r in cur.fetchall()]
        cur.execute("SELECT product_id FROM products WHERE stock > 5")
        products = [r["product_id"] for r in cur.fetchall()]
    for _ in range(n):
        items = [{"product_id": p, "quantity": random.randint(1, 2)}
                 for p in random.sample(products, k=min(len(products), random.randint(1, 3)))]
        try:
            with transaction() as cur:  # one transaction per order
                create_order_tx(cur, random.choice(customers), items, random.choice(PAYMENT_METHODS))
            created += 1
        except ApiError:
            failed += 1
    with transaction() as cur:
        # Move a few not-yet-delivered orders one step forward in the life-cycle.
        cur.execute(
            """
            UPDATE orders o SET status = CASE o.status
                     WHEN 'pending' THEN 'processing'
                     WHEN 'processing' THEN 'shipped'
                     ELSE 'delivered' END
             WHERE o.order_id IN (SELECT order_id FROM orders WHERE status <> 'delivered'
                                  ORDER BY random() LIMIT %s)
            """,
            (n,),
        )
        advanced = cur.rowcount
    return jsonify({"orders_created": created, "orders_advanced": advanced,
                    "rejected": failed, "committed_at": committed_at()})


# ------------------------------------------------ direct (naive) reads
ORDER_JOIN_SQL = """
    SELECT o.order_id, o.order_date, o.status, o.total_amount,
           json_build_object('customer_id', c.customer_id, 'name', c.name,
                             'email', c.email, 'city', c.city) AS customer,
           COALESCE((SELECT json_agg(json_build_object(
                         'product_id', p.product_id, 'product_name', p.name,
                         'category', cat.name, 'quantity', oi.quantity,
                         'unit_price', oi.unit_price,
                         'line_total', oi.quantity * oi.unit_price) ORDER BY oi.order_item_id)
                     FROM order_items oi
                     JOIN products p     ON p.product_id = oi.product_id
                     JOIN categories cat ON cat.category_id = p.category_id
                     WHERE oi.order_id = o.order_id), '[]') AS items,
           (SELECT row_to_json(pay) FROM (
                SELECT method, amount, status, paid_at FROM payments
                WHERE order_id = o.order_id ORDER BY payment_id DESC LIMIT 1) pay) AS payment,
           COALESCE((SELECT json_agg(json_build_object(
                         'old_status', h.old_status, 'new_status', h.new_status,
                         'changed_at', h.changed_at) ORDER BY h.changed_at, h.history_id)
                     FROM order_status_history h WHERE h.order_id = o.order_id), '[]') AS history
    FROM orders o
    JOIN customers c ON c.customer_id = o.customer_id
    WHERE o.order_id = %s
"""


@app.get("/api/write/direct/orders/<int:order_id>")
def direct_order(order_id):
    with transaction() as cur:
        t0 = time.perf_counter()
        cur.execute(ORDER_JOIN_SQL, (order_id,))
        row = cur.fetchone()
        db_ms = (time.perf_counter() - t0) * 1000
    if row is None:
        raise ApiError("order not found", 404)
    resp = jsonify(row)
    resp.headers["X-Read-Source"] = "postgres"
    resp.headers["Server-Timing"] = f"db;dur={db_ms:.3f}"   # time spent in the data store only
    return resp


@app.get("/api/write/direct/orders")
def direct_orders():
    with transaction() as cur:
        cur.execute("SELECT order_id, status, total_amount FROM orders ORDER BY order_id")
        rows = cur.fetchall()
    return jsonify(rows)


@app.get("/api/write/direct/lookups")
def lookups():
    with transaction() as cur:
        cur.execute("SELECT customer_id, name, city FROM customers ORDER BY customer_id")
        customers = cur.fetchall()
        cur.execute(
            "SELECT p.product_id, p.name, p.price, p.stock, c.name AS category "
            "FROM products p JOIN categories c USING (category_id) ORDER BY p.product_id"
        )
        products = cur.fetchall()
    return jsonify({"customers": customers, "products": products})


# --------------------------------------------- dual-write problem demo
# This is deliberately the WRONG design, kept here only to demonstrate the
# problem on stage. It writes to a separate "naive:" key space so it never
# touches the real CDC read model.
_redis = None


def naive_cache():
    global _redis
    if _redis is None:
        import redis

        _redis = redis.Redis(host=os.environ.get("REDIS_HOST", "localhost"),
                             port=int(os.environ.get("REDIS_PORT", "6379")), decode_responses=True)
    return _redis


@app.post("/api/write/demo/dual-write")
def dual_write():
    d = body()
    order_id, status, crash = int(d["order_id"]), d.get("status"), bool(d.get("crash"))
    if status not in STATUSES:
        raise ApiError(f"status must be one of {STATUSES}")
    key = f"naive:order:{order_id}"

    # Step 1: write to Postgres (commits).
    with transaction() as cur:
        cur.execute("UPDATE orders SET status = %s WHERE order_id = %s", (status, order_id))
        if cur.rowcount == 0:
            raise ApiError("order not found", 404)

    # Step 2: write to Redis. If the process dies between step 1 and step 2
    # there is no transaction spanning both systems, so nobody fixes the cache.
    if not crash:
        naive_cache().set(key, json.dumps({"order_id": order_id, "status": status}))

    with transaction() as cur:
        cur.execute("SELECT status FROM orders WHERE order_id = %s", (order_id,))
        pg_status = cur.fetchone()["status"]
    cached = naive_cache().get(key)
    return jsonify({
        "order_id": order_id,
        "postgres_status": pg_status,
        "naive_cache_status": json.loads(cached)["status"] if cached else None,
        "crashed_before_cache_write": crash,
        "committed_at": committed_at(),
    })


@app.get("/api/write/health")
def health():
    with transaction() as cur:
        cur.execute("SELECT 1 AS ok")
    return jsonify({"ok": True, "store": "postgres"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5001")), debug=False, threaded=True)
