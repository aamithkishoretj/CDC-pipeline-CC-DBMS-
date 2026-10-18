"""
Read-path benchmark: PostgreSQL (normalized, JOIN at read time)
                 vs  Redis      (denormalized document built by CDC)

Measures the data stores directly (no HTTP). Each reader is a separate
process (not a thread), so Python's GIL in the benchmark client does not
distort the result. Optionally W writer processes hammer Postgres at the same
time, to recreate "heavy reads compete with writes" from the problem statement.

    pip install -r benchmark/requirements.txt
    python benchmark/benchmark.py                          # reads only
    python benchmark/benchmark.py --writers 4              # reads under write load
    python benchmark/benchmark.py --reads 5000 --clients 8 --csv results.csv
"""

import argparse
import csv
import multiprocessing as mp
import random
import statistics
import time

import psycopg2
import redis

ORDER_SQL = """
SELECT o.order_id, o.order_date, o.status, o.total_amount,
       c.name, c.email, c.city,
       oi.quantity, oi.unit_price, p.name AS product, cat.name AS category,
       pay.method, pay.status AS payment_status,
       (SELECT json_agg(h ORDER BY h.changed_at) FROM order_status_history h
         WHERE h.order_id = o.order_id) AS history
FROM orders o
JOIN customers c     ON c.customer_id = o.customer_id
JOIN order_items oi  ON oi.order_id   = o.order_id
JOIN products p      ON p.product_id  = oi.product_id
JOIN categories cat  ON cat.category_id = p.category_id
LEFT JOIN payments pay ON pay.order_id = o.order_id
WHERE o.order_id = %s
"""


def pg_connect(cfg):
    conn = psycopg2.connect(host=cfg["pg_host"], port=cfg["pg_port"],
                            user="cdcuser", password="cdcpass", dbname="shopdb")
    conn.autocommit = True
    return conn


def percentile(values, p):
    s = sorted(values)
    return s[min(len(s) - 1, int(p / 100 * len(s)))]


def reader(job):
    """Runs in its own process; returns per-read latencies in ms."""
    path, cfg, ids, n = job
    if path == "postgres_join":
        cur = pg_connect(cfg).cursor()
        def read(oid):
            cur.execute(ORDER_SQL, (oid,))
            cur.fetchall()
    else:
        rc = redis.Redis(host=cfg["redis_host"], port=cfg["redis_port"])
        def read(oid):
            rc.get(f"view:order:{oid}")
    rng = random.Random()
    out = []
    for _ in range(n):
        oid = rng.choice(ids)
        t = time.perf_counter()
        read(oid)
        out.append((time.perf_counter() - t) * 1000)
    return out


def writer(cfg, stop):
    cur = pg_connect(cfg).cursor()
    while not stop.is_set():
        cur.execute(
            "UPDATE orders SET status = (ARRAY['pending','processing','shipped','delivered'])[1 + floor(random()*4)::int] "
            "WHERE order_id = %s",
            (random.randint(1, 1200),),
        )
        cur.execute("UPDATE products SET stock = stock WHERE product_id = %s", (random.randint(1, 24),))


def run(pool, path, cfg, ids, total, clients):
    per = max(1, total // clients)
    t0 = time.perf_counter()
    timings = [x for part in pool.map(reader, [(path, cfg, ids, per)] * clients) for x in part]
    wall = time.perf_counter() - t0
    return {
        "path": path,
        "reads": len(timings),
        "avg_ms": statistics.mean(timings),
        "p50_ms": percentile(timings, 50),
        "p95_ms": percentile(timings, 95),
        "p99_ms": percentile(timings, 99),
        "reads_per_s": len(timings) / wall,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reads", type=int, default=2000, help="reads per path")
    ap.add_argument("--clients", "--threads", type=int, default=4, help="concurrent reader processes")
    ap.add_argument("--writers", type=int, default=0, help="concurrent writer processes")
    ap.add_argument("--pg-host", default="localhost")
    ap.add_argument("--pg-port", type=int, default=5433)
    ap.add_argument("--redis-host", default="localhost")
    ap.add_argument("--redis-port", type=int, default=6379)
    ap.add_argument("--csv")
    a = ap.parse_args()
    cfg = {"pg_host": a.pg_host, "pg_port": a.pg_port, "redis_host": a.redis_host, "redis_port": a.redis_port}

    ids = [int(x) for x in redis.Redis(host=a.redis_host, port=a.redis_port).zrange("orders:recent", 0, -1)]
    if not ids:
        raise SystemExit("Redis read model is empty: is the stack running and the connector registered?")

    stop = mp.Event()
    writers = [mp.Process(target=writer, args=(cfg, stop), daemon=True) for _ in range(a.writers)]
    for w in writers:
        w.start()

    print(f"{len(ids)} orders, {a.reads} reads per path, {a.clients} reader processes, {a.writers} writer processes\n")
    with mp.Pool(a.clients) as pool:
        run(pool, "postgres_join", cfg, ids, 400, a.clients)    # warm-up both paths
        run(pool, "redis_view", cfg, ids, 400, a.clients)
        results = [
            run(pool, "postgres_join", cfg, ids, a.reads, a.clients),
            run(pool, "redis_view", cfg, ids, a.reads, a.clients),
        ]
    stop.set()
    for w in writers:
        w.join(timeout=5)

    print(f"{'path':<16}{'avg ms':>9}{'p50 ms':>9}{'p95 ms':>9}{'p99 ms':>9}{'reads/s':>11}")
    for res in results:
        print(f"{res['path']:<16}{res['avg_ms']:>9.2f}{res['p50_ms']:>9.2f}{res['p95_ms']:>9.2f}{res['p99_ms']:>9.2f}{res['reads_per_s']:>11.0f}")
    pg, rd = results
    print(f"\nRedis: {pg['avg_ms'] / rd['avg_ms']:.1f}x lower avg latency, {rd['reads_per_s'] / pg['reads_per_s']:.1f}x throughput")

    if a.csv:
        with open(a.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0]) + ["writers"])
            w.writeheader()
            for res in results:
                w.writerow({**res, "writers": a.writers})
        print(f"Saved {a.csv}")


if __name__ == "__main__":
    main()
