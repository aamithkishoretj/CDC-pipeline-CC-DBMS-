"""
CDC Consumer / Projector:  Kafka (Debezium change events)  ->  Redis read model

This service is the ONLY writer of the Redis read model. The application
writes to PostgreSQL only; Debezium turns every committed row change into a
Kafka event, and this consumer projects those events into Redis.

Two layers are kept in Redis:

  1. raw:<table>:<pk>          a 1:1 replica of each Postgres row (JSON)
     idx:...                   reverse indexes (customer -> orders, ...)

  2. view:order:<id>           DENORMALIZED documents built for the Read API:
     view:customer:<id>        an order view already contains the customer,
     view:product:<id>         every item with its product name, the payment
                               and the full status history -> 1 GET per read,
                               no joins at read time.

Delivery guarantees
  * Offsets are committed manually, only AFTER Redis has been updated
    -> at-least-once delivery (a crash means events are re-processed).
  * Every Redis write is an absolute SET/HSET/ZADD/SADD (never an increment),
    so re-processing an event is harmless -> the projection is idempotent.
  * Kafka keys events by primary key, so all changes to one row land in the
    same partition and are applied in commit order.

Debezium envelope:  {"before": {...}, "after": {...}, "op": "c|u|d|r",
                     "source": {"table": "...", "ts_ms": ...}, "ts_ms": ...}
"""

import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import redis
from confluent_kafka import Consumer, KafkaError, KafkaException
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cdc-consumer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "shopdb")
GROUP_ID = os.environ.get("GROUP_ID", "cdc-redis-projector")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8000"))

PK = {
    "categories": "category_id",
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "order_status_history": "history_id",
}
STATUSES = ("pending", "processing", "shipped", "delivered")
TIMESTAMP_FIELDS = {"created_at", "updated_at", "order_date", "paid_at", "changed_at"}
MONEY_FIELDS = {"price", "total_amount", "unit_price", "amount"}
EVENT_LOG_SIZE = 200

# ---------------------------------------------------------------- metrics
EVENTS = Counter("cdc_events_total", "CDC events applied to Redis", ["table", "op"])
LAG = Histogram(
    "cdc_end_to_end_lag_seconds",
    "Postgres commit -> Redis applied latency",
    buckets=(0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
VIEW_REBUILDS = Counter("cdc_view_rebuilds_total", "Denormalized views rebuilt", ["view"])
KAFKA_LAG = Gauge("cdc_kafka_consumer_lag", "Events in Kafka not yet applied to Redis", ["table"])
REDIS_HITS = Gauge("cdc_redis_keyspace_hits", "Redis keyspace hits (INFO stats)")
REDIS_MISSES = Gauge("cdc_redis_keyspace_misses", "Redis keyspace misses (INFO stats)")
REDIS_KEYS = Gauge("cdc_redis_keys", "Keys in the Redis read model")
LAST_EVENT = Gauge("cdc_last_event_timestamp_seconds", "Wall-clock time of last applied event")


# ---------------------------------------------------------------- helpers
def now_ms() -> int:
    return int(time.time() * 1000)


def normalize(row: dict | None) -> dict | None:
    """Convert Debezium wire types into plain JSON values."""
    if row is None:
        return None
    out = dict(row)
    for k, v in out.items():
        if v is None:
            continue
        if k in TIMESTAMP_FIELDS and isinstance(v, (int, float)):
            # TIMESTAMP (no tz) arrives as microseconds since epoch.
            out[k] = datetime.fromtimestamp(v / 1_000_000, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
        elif k in MONEY_FIELDS:
            out[k] = round(float(v), 2)  # DECIMAL arrives as a string (decimal.handling.mode=string)
    return out


def epoch(ts: str | None) -> float:
    if not ts:
        return 0.0
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()


def jloads(s):
    return json.loads(s) if s else None


class Dirty:
    """Views that must be rebuilt after a batch of raw changes is applied."""

    def __init__(self):
        self.orders: set[int] = set()
        self.customers: set[int] = set()
        self.products: set[int] = set()

    def __bool__(self):
        return bool(self.orders or self.customers or self.products)


# ---------------------------------------------------------- raw projection
def apply_event(r: redis.Redis, pipe, table: str, event: dict, dirty: Dirty):
    """Apply one change event to the raw replica + indexes (writes go to `pipe`)."""
    op = event["op"]
    before = normalize(event.get("before"))
    after = normalize(event.get("after"))
    row = after if op in ("c", "u", "r") else before
    if row is None:
        return None
    pk = row[PK[table]]
    raw_key = f"raw:{table}:{pk}"

    if op == "d":
        pipe.delete(raw_key)
    else:
        pipe.set(raw_key, json.dumps(after))

    if table == "categories":
        dirty.products.update(int(x) for x in r.smembers(f"idx:category:{pk}:products"))

    elif table == "products":
        if before and (not after or before["category_id"] != after["category_id"]):
            pipe.srem(f"idx:category:{before['category_id']}:products", pk)
        if op == "d":
            pipe.zrem("products:all", pk)
        else:
            pipe.sadd(f"idx:category:{after['category_id']}:products", pk)
            pipe.zadd("products:all", {pk: pk})
        dirty.products.add(pk)
        # Order views embed the product name/category; stock changes on every
        # sale, so only fan out to orders when a displayed field changed.
        if not before or not after or before["name"] != after["name"] or before["category_id"] != after["category_id"]:
            dirty.orders.update(int(x) for x in r.smembers(f"idx:product:{pk}:orders"))

    elif table == "customers":
        if op == "d":
            pipe.zrem("customers:all", pk)
        else:
            pipe.zadd("customers:all", {pk: pk})
        dirty.customers.add(pk)
        if not before or not after or any(before[f] != after[f] for f in ("name", "email", "city")):
            dirty.orders.update(int(x) for x in r.smembers(f"idx:customer:{pk}:orders"))

    elif table == "orders":
        if before and (not after or before["customer_id"] != after["customer_id"]):
            pipe.srem(f"idx:customer:{before['customer_id']}:orders", pk)
            dirty.customers.add(before["customer_id"])
        if after:
            pipe.sadd(f"idx:customer:{after['customer_id']}:orders", pk)
            dirty.customers.add(after["customer_id"])
        dirty.orders.add(pk)

    elif table == "order_items":
        oid = row["order_id"]
        if op == "d":
            pipe.srem(f"idx:order:{oid}:items", pk)
            pipe.srem(f"idx:product:{row['product_id']}:orders", oid)
        else:
            pipe.sadd(f"idx:order:{oid}:items", pk)
            pipe.sadd(f"idx:product:{row['product_id']}:orders", oid)
            if before and before["product_id"] != after["product_id"]:
                pipe.srem(f"idx:product:{before['product_id']}:orders", oid)
        dirty.orders.add(oid)

    elif table == "payments":
        idx = f"idx:order:{row['order_id']}:payments"
        pipe.srem(idx, pk) if op == "d" else pipe.sadd(idx, pk)
        dirty.orders.add(row["order_id"])

    elif table == "order_status_history":
        idx = f"idx:order:{row['order_id']}:history"
        pipe.srem(idx, pk) if op == "d" else pipe.sadd(idx, pk)
        dirty.orders.add(row["order_id"])

    return pk


# ------------------------------------------------------- view materializers
def mget_json(r: redis.Redis, keys: list[str]) -> list:
    return [jloads(v) for v in r.mget(keys)] if keys else []


def rebuild_order(r: redis.Redis, oid: int):
    order = jloads(r.get(f"raw:orders:{oid}"))
    pipe = r.pipeline(transaction=True)
    if order is None:
        pipe.delete(f"view:order:{oid}")
        pipe.zrem("orders:recent", oid)
        for s in STATUSES:
            pipe.zrem(f"orders:status:{s}", oid)
        pipe.hdel("orders:totals", oid)
        pipe.execute()
        VIEW_REBUILDS.labels("order_deleted").inc()
        return

    p = r.pipeline(transaction=False)
    p.get(f"raw:customers:{order['customer_id']}")
    p.smembers(f"idx:order:{oid}:items")
    p.smembers(f"idx:order:{oid}:payments")
    p.smembers(f"idx:order:{oid}:history")
    cust_raw, item_ids, pay_ids, hist_ids = p.execute()
    customer = jloads(cust_raw)

    items = [i for i in mget_json(r, [f"raw:order_items:{i}" for i in item_ids]) if i]
    products = mget_json(r, [f"raw:products:{i['product_id']}" for i in items])
    cat_ids = {p["category_id"] for p in products if p}
    categories = {c["category_id"]: c["name"] for c in mget_json(r, [f"raw:categories:{c}" for c in cat_ids]) if c}
    payments = [x for x in mget_json(r, [f"raw:payments:{i}" for i in pay_ids]) if x]
    history = [x for x in mget_json(r, [f"raw:order_status_history:{i}" for i in hist_ids]) if x]

    view_items = []
    for item, prod in zip(items, products):
        view_items.append({
            "order_item_id": item["order_item_id"],
            "product_id": item["product_id"],
            "product_name": prod["name"] if prod else f"Product #{item['product_id']}",
            "category": categories.get(prod["category_id"]) if prod else None,
            "quantity": item["quantity"],
            "unit_price": item["unit_price"],
            "line_total": round(item["quantity"] * item["unit_price"], 2),
        })
    view_items.sort(key=lambda x: x["order_item_id"])
    payments.sort(key=lambda x: x["payment_id"])
    history.sort(key=lambda x: (x["changed_at"] or "", x["history_id"]))

    view = {
        "order_id": oid,
        "order_date": order["order_date"],
        "status": order["status"],
        "total_amount": order["total_amount"],
        "customer": {
            "customer_id": order["customer_id"],
            "name": customer["name"] if customer else None,
            "email": customer["email"] if customer else None,
            "city": customer["city"] if customer else None,
        },
        "items": view_items,
        "item_count": sum(i["quantity"] for i in view_items),
        "payment": payments[-1] if payments else None,
        "history": [
            {"old_status": h["old_status"], "new_status": h["new_status"], "changed_at": h["changed_at"]}
            for h in history
        ],
        "projected_at": now_ms(),
    }

    score = epoch(order["order_date"])
    pipe.set(f"view:order:{oid}", json.dumps(view))
    pipe.zadd("orders:recent", {oid: score})
    for s in STATUSES:
        if s == order["status"]:
            pipe.zadd(f"orders:status:{s}", {oid: score})
        else:
            pipe.zrem(f"orders:status:{s}", oid)
    pipe.hset("orders:totals", oid, order["total_amount"])
    pipe.execute()
    VIEW_REBUILDS.labels("order").inc()


def rebuild_customer(r: redis.Redis, cid: int):
    customer = jloads(r.get(f"raw:customers:{cid}"))
    if customer is None:
        r.delete(f"view:customer:{cid}")
        return
    order_ids = sorted(int(x) for x in r.smembers(f"idx:customer:{cid}:orders"))
    totals, dates = [], []
    if order_ids:
        totals = r.hmget("orders:totals", order_ids)
        p = r.pipeline(transaction=False)
        for oid in order_ids:
            p.zscore("orders:recent", oid)
        dates = p.execute()
    known = [d for d in dates if d]
    view = {
        **customer,
        "order_count": len(order_ids),
        "total_spent": round(sum(float(t) for t in totals if t), 2),
        "last_order_date": (
            datetime.fromtimestamp(max(known), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") if known else None
        ),
        "order_ids": order_ids[-20:],
    }
    r.set(f"view:customer:{cid}", json.dumps(view))
    VIEW_REBUILDS.labels("customer").inc()


def rebuild_product(r: redis.Redis, pid: int):
    product = jloads(r.get(f"raw:products:{pid}"))
    if product is None:
        r.delete(f"view:product:{pid}")
        return
    category = jloads(r.get(f"raw:categories:{product['category_id']}"))
    view = {
        **product,
        "category": category["name"] if category else None,
        "orders_containing": r.scard(f"idx:product:{pid}:orders"),
    }
    r.set(f"view:product:{pid}", json.dumps(view))
    VIEW_REBUILDS.labels("product").inc()


def rebuild_dirty(r: redis.Redis, dirty: Dirty):
    # Orders first: customer views read order totals/dates the order rebuild writes.
    for oid in sorted(dirty.orders):
        rebuild_order(r, oid)
    for cid in sorted(dirty.customers):
        rebuild_customer(r, cid)
    for pid in sorted(dirty.products):
        rebuild_product(r, pid)


# ------------------------------------------------------------------- main
def connect_kafka() -> Consumer:
    """confluent-kafka (librdkafka) consumer. librdkafka keeps retrying unreachable
    brokers in the background and follows partition leaders when a broker fails,
    so losing one of the three brokers does not stall consumption."""
    pattern = rf"^{TOPIC_PREFIX}\.public\..*"
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,                 # commit only after Redis is updated
        "topic.metadata.refresh.interval.ms": 5000,  # discover new topics and new leaders quickly
        "fetch.wait.max.ms": 100,                    # low latency instead of the 500 ms default
        "session.timeout.ms": 10000,
    })
    consumer.subscribe([pattern])
    log.info("Subscribed to /%s/ on %s", pattern, KAFKA_BOOTSTRAP)
    return consumer


def connect_redis() -> redis.Redis:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    while True:
        try:
            r.ping()
            log.info("Connected to Redis at %s:%s", REDIS_HOST, REDIS_PORT)
            return r
        except redis.ConnectionError:
            log.warning("Redis not reachable yet, retrying in 3s ...")
            time.sleep(3)


def process_batch(r: redis.Redis, messages) -> int:
    dirty = Dirty()
    pipe = r.pipeline(transaction=False)
    feed, applied = [], 0
    stats = {}

    for msg in messages:
        if msg.error():
            code = msg.error().code()
            if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                log.info("Waiting for Debezium to create the change topics ...")
            elif code != KafkaError._PARTITION_EOF:
                log.warning("Kafka error: %s", msg.error())
            continue
        table = msg.topic().split(".")[-1]
        if table not in PK or not msg.value():          # unknown topic, or tombstone after a delete
            continue
        try:
            event = json.loads(msg.value())
            if "op" not in event:
                continue
            pk = apply_event(r, pipe, table, event, dirty)
        except (KeyError, TypeError, ValueError):
            # A malformed event must not block the partition forever.
            log.exception("Skipping malformed event %s@%d", msg.topic(), msg.offset())
            continue
        if pk is None:
            continue
        applied += 1
        op = event["op"]
        src_ts = (event.get("source") or {}).get("ts_ms") or event.get("ts_ms") or now_ms()
        lag = max(0, now_ms() - src_ts)
        EVENTS.labels(table, op).inc()
        if op != "r":
            LAG.observe(lag / 1000)
        stats[f"events:{table}"] = stats.get(f"events:{table}", 0) + 1
        feed.append({
            "table": table,
            "op": op,
            "pk": pk,
            "partition": msg.partition(),
            "offset": msg.offset(),
            "source_ts": src_ts,
            "lag_ms": lag,
        })

    pipe.execute()          # 1. raw replica + indexes
    rebuild_dirty(r, dirty)  # 2. denormalized read views

    if feed:                 # 3. pipeline telemetry for the dashboard
        done = now_ms()
        p = r.pipeline(transaction=False)
        for item in feed:
            item["applied_at"] = done
            item["lag_ms"] = max(0, done - item["source_ts"])
            p.lpush("cdc:events", json.dumps(item))
        p.ltrim("cdc:events", 0, EVENT_LOG_SIZE - 1)
        for k, v in stats.items():
            p.hincrby("cdc:stats", k, v)
        p.hincrby("cdc:stats", "events_total", len(feed))
        live = [i for i in feed if i["op"] != "r"]
        if live:
            p.hset("cdc:stats", "last_lag_ms", live[-1]["lag_ms"])
        p.hset("cdc:stats", "last_event_at", done)
        p.execute()
        LAST_EVENT.set(done / 1000)
    return applied


def export_gauges(r: redis.Redis, consumer: Consumer):
    """Kafka consumer lag (high watermark - our position) and Redis hit/miss counters."""
    assigned = consumer.assignment()
    if assigned:
        per_table = {}
        for tp in consumer.position(assigned):
            # cached=True: the high watermark comes from the latest fetch response,
            # so this never blocks on a broker (important while one is down).
            low, high = consumer.get_watermark_offsets(tp, cached=True)
            table = tp.topic.split(".")[-1]
            lag = high - tp.offset if high >= 0 and tp.offset >= 0 else 0
            per_table[table] = per_table.get(table, 0) + max(0, lag)
        for table, lag in per_table.items():
            KAFKA_LAG.labels(table).set(lag)
    info = r.info("stats")
    REDIS_HITS.set(info.get("keyspace_hits", 0))
    REDIS_MISSES.set(info.get("keyspace_misses", 0))
    REDIS_KEYS.set(r.dbsize())


def main():
    start_http_server(METRICS_PORT)
    log.info("Prometheus metrics on :%d/metrics", METRICS_PORT)
    r = connect_redis()
    consumer = connect_kafka()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    next_gauges = 0.0
    while running:
        if time.time() >= next_gauges:
            next_gauges = time.time() + 5
            try:
                export_gauges(r, consumer)
            except Exception:  # metrics must never stop the pipeline
                log.debug("gauge export failed", exc_info=True)
        # Block only until the FIRST message arrives, then drain whatever is already
        # buffered (timeout=0). consume(500, timeout=1) would wait up to 1 s to fill a batch.
        first = consumer.poll(1.0)
        if first is None:
            continue
        messages = [first] + consumer.consume(num_messages=499, timeout=0)
        try:
            n = process_batch(r, messages)
            # At-least-once: offsets move only after Redis is updated. A batch made only
            # of error events (e.g. "topic not created yet" at startup) has nothing to commit.
            if any(m.error() is None for m in messages):
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException as e:
                    if e.args[0].code() != KafkaError._NO_OFFSET:
                        raise
            if n:
                log.info("Applied %d change events", n)
        except redis.ConnectionError:
            # Nothing was committed for this batch. Re-joining the group resumes
            # from the last committed offsets, so the batch is replayed (idempotently).
            log.exception("Redis unavailable; replaying from last committed offsets")
            consumer.close()
            time.sleep(3)
            r = connect_redis()
            consumer = connect_kafka()

    log.info("Shutting down consumer ...")
    consumer.close()


if __name__ == "__main__":
    main()
