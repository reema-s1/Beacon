"""Build the daily sales report file into the shared reports volume."""
from __future__ import annotations

import csv
import io
import shutil
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from platform_lib import DEFAULT_ARGS, REPORTS_DIR, WAREHOUSE_CONN_ID, log, log_sla_miss

KEEP_REPORTS = 2
DETAIL_ROWS = 20000  # reconciliation detail section, roughly 1 MB per report


@dag(
    dag_id="report_export",
    schedule="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=log_sla_miss,
    tags=["reporting"],
)
def report_export():
    @task
    def prune_old_reports() -> int:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        reports = sorted(REPORTS_DIR.glob("sales_report_*.csv"))
        stale = reports[:-KEEP_REPORTS] if len(reports) > KEEP_REPORTS else []
        for old in stale:
            old.unlink()
            log.info("Pruned old report %s", old.name)
        return len(stale)

    @task
    def build_report() -> str:
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        hook.run("""CREATE TABLE IF NOT EXISTS sales_daily (
                      order_date date, region text, orders int, revenue numeric,
                      loaded_at timestamptz DEFAULT now(), PRIMARY KEY (order_date, region))""")
        summary = hook.get_records("SELECT order_date, region, orders, revenue FROM sales_daily ORDER BY 1, 2")
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["order_date", "region", "orders", "revenue"])
        w.writerows(summary)
        w.writerow([])
        w.writerow(["detail_line", "note"])
        for i in range(DETAIL_ROWS):
            w.writerow([i, "reconciled against ledger snapshot"])
        log.info("Built report with %d summary rows (%d bytes)", len(summary), len(buf.getvalue()))
        return buf.getvalue()

    @task(sla=timedelta(minutes=8))
    def write_report(content: str, ts_nodash: str | None = None) -> str:
        target = REPORTS_DIR / f"sales_report_{ts_nodash}.csv"
        usage = shutil.disk_usage(REPORTS_DIR)
        log.info("Writing %s (%d bytes); volume %s free=%d bytes of %d",
                 target, len(content), REPORTS_DIR, usage.free, usage.total)
        try:
            target.write_text(content)
        except OSError:
            target.unlink(missing_ok=True)
            raise
        log.info("Report written to %s", target)
        return str(target)

    report = build_report()
    prune_old_reports() >> report
    write_report(report)


report_export()
