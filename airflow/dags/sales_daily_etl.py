"""Sales ETL: extract the upstream CSV drop -> clean -> aggregate -> load to the warehouse."""
from __future__ import annotations

import csv
import shutil
from collections import defaultdict
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from platform_lib import (
    DEFAULT_ARGS, INCOMING_DIR, STAGING_DIR, fault, log, log_sla_miss, warehouse_conn_id,
)


@dag(
    dag_id="sales_daily_etl",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=log_sla_miss,
    tags=["sales", "etl"],
)
def sales_daily_etl():
    @task
    def extract(ts_nodash: str | None = None) -> str:
        source = INCOMING_DIR / "sales.csv"
        log.info("Reading upstream sales drop from %s", source)
        if not source.exists():
            present = sorted(p.name for p in INCOMING_DIR.iterdir()) if INCOMING_DIR.exists() else []
            log.error("Input file %s does not exist. Files present in %s: %s", source, INCOMING_DIR, present)
            raise FileNotFoundError(f"Expected input file not found: {source}")
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staged = STAGING_DIR / f"sales_{ts_nodash}.csv"
        shutil.copyfile(source, staged)
        log.info("Staged %s (%d bytes)", staged, staged.stat().st_size)
        return str(staged)

    @task
    def clean(staged_path: str) -> list[dict]:
        rows, dropped = [], 0
        price_field = "unit_price_usd" if fault("bad_code") else "unit_price"
        with open(staged_path, newline="") as f:
            for row in csv.DictReader(f):
                price = float(row[price_field])
                qty = int(row["quantity"])
                if qty <= 0 or price <= 0:
                    dropped += 1
                    continue
                rows.append({"order_date": row["order_date"], "region": row["region"],
                             "product": row["product"], "quantity": qty, "unit_price": price})
        log.info("Cleaned %d rows, dropped %d invalid rows", len(rows), dropped)
        return rows

    @task
    def aggregate(rows: list[dict]) -> list[dict]:
        totals: dict[tuple, dict] = defaultdict(lambda: {"orders": 0, "revenue": 0.0})
        for r in rows:
            key = (r["order_date"], r["region"])
            totals[key]["orders"] += 1
            totals[key]["revenue"] += r["quantity"] * r["unit_price"]
        out = [{"order_date": d, "region": reg, "orders": v["orders"], "revenue": round(v["revenue"], 2)}
               for (d, reg), v in sorted(totals.items())]
        log.info("Aggregated into %d region/day rows", len(out))
        return out

    @task(sla=timedelta(minutes=4))
    def load(agg: list[dict]) -> int:
        conn_id = warehouse_conn_id()
        log.info("Loading %d rows into warehouse via connection '%s'", len(agg), conn_id)
        hook = PostgresHook(postgres_conn_id=conn_id)
        hook.run("""CREATE TABLE IF NOT EXISTS sales_daily (
                      order_date date, region text, orders int, revenue numeric,
                      loaded_at timestamptz DEFAULT now(), PRIMARY KEY (order_date, region))""")
        hook.insert_rows("sales_daily", [(r["order_date"], r["region"], r["orders"], r["revenue"]) for r in agg],
                         target_fields=["order_date", "region", "orders", "revenue"],
                         replace=True, replace_index=["order_date", "region"])
        log.info("Load complete")
        return len(agg)

    load(aggregate(clean(extract())))


sales_daily_etl()
