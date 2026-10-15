"""Create the input file that `sales_daily_etl` expects from its upstream drop."""
import csv
import random
from datetime import date

from platform_lib import INCOMING_DIR, STAGING_DIR

REGIONS = ["north", "south", "east", "west"]
PRODUCTS = [("widget", 4.50), ("gadget", 12.00), ("gizmo", 7.25), ("doohickey", 2.10)]


def write_sales_csv(path, rows: int = 500, seed: int = 7) -> None:
    rng = random.Random(seed)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "order_date", "region", "product", "quantity", "unit_price"])
        for i in range(rows):
            product, price = rng.choice(PRODUCTS)
            w.writerow([f"ord-{i:05d}", date.today().isoformat(), rng.choice(REGIONS),
                        product, rng.randint(1, 9), f"{price:.2f}"])


if __name__ == "__main__":
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    target = INCOMING_DIR / "sales.csv"
    for stale in INCOMING_DIR.glob("sales.csv.*"):
        stale.unlink()
    write_sales_csv(target)
    print(f"seeded {target}")
