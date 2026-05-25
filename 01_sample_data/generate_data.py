"""
Stage 1: Generate realistic e-commerce sample data for the migration pipeline.

Produces:
  - orders.csv       (100K rows, ~15 MB)
  - events.csv       (500K rows, ~35 MB)
  - user_segments.csv (1K rows, ~20 KB)

Also writes Parquet versions for direct Spark testing.

Intentionally includes data quality issues:
  - ~0.5% null user_ids in orders
  - ~1% duplicate event_ids in events
  - ~2 orders with negative amounts
"""

import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Config ──────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "output_data"
NUM_ORDERS = 100_000
NUM_EVENTS = 500_000
NUM_USERS = 10_000
NUM_SEGMENTS = 1_000
RUN_DATE = datetime.now().strftime("%Y-%m-%d")
EVENT_TYPES = ["purchase", "cart_add", "page_view", "wishlist", "search"]
CURRENCIES = ["USD", "EUR", "GBP", "INR", "JPY"]
SEGMENTS = ["premium", "standard", "budget", "new", "churned"]


def generate_user_pool():
    """Generate a pool of user IDs to reference across tables."""
    return [f"user_{i:06d}" for i in range(NUM_USERS)]


def generate_orders(users):
    """Generate order data with intentional quality issues."""
    print(f"  Generating {NUM_ORDERS:,} orders...")
    rows = []
    for i in range(NUM_ORDERS):
        user_id = random.choice(users)

        # Intentional: ~0.5% null user_ids
        if random.random() < 0.005:
            user_id = None

        amount = round(random.uniform(1.0, 500.0), 2)

        # Intentional: 2 negative amounts (data quality issue)
        if i in (42, 9999):
            amount = round(random.uniform(-100.0, -1.0), 2)

        rows.append({
            "order_id": f"ORD-{uuid.uuid4().hex[:12].upper()}",
            "user_id": user_id,
            "amount": amount,
            "currency": random.choice(CURRENCIES),
            "status": random.choice(["completed", "pending", "refunded"]),
            "created_at": fake.date_time_between(
                start_date="-7d", end_date="now"
            ).isoformat(),
            "process_date": RUN_DATE,
        })

    return pd.DataFrame(rows)


def generate_events(users):
    """Generate event data with intentional duplicates."""
    print(f"  Generating {NUM_EVENTS:,} events...")
    rows = []
    for i in range(NUM_EVENTS):
        event_id = f"EVT-{uuid.uuid4().hex[:12].upper()}"

        # Intentional: ~1% duplicate event_ids (re-use previous event_id)
        if random.random() < 0.01 and len(rows) > 0:
            event_id = rows[-1]["event_id"]

        rows.append({
            "event_id": event_id,
            "user_id": random.choice(users),
            "event_type": random.choice(EVENT_TYPES),
            "page_url": fake.uri_path(),
            "session_id": f"SES-{uuid.uuid4().hex[:8]}",
            "timestamp": fake.date_time_between(
                start_date="-7d", end_date="now"
            ).isoformat(),
            "process_date": RUN_DATE,
        })

    return pd.DataFrame(rows)


def generate_user_segments(users):
    """Generate user segment dimension table."""
    print(f"  Generating {NUM_SEGMENTS:,} user segments...")
    sampled = random.sample(users, min(NUM_SEGMENTS, len(users)))
    rows = []
    for user_id in sampled:
        rows.append({
            "user_id": user_id,
            "segment": random.choice(SEGMENTS),
            "lifetime_value": round(random.uniform(10.0, 10000.0), 2),
            "signup_date": fake.date_between(
                start_date="-2y", end_date="today"
            ).isoformat(),
            "country": fake.country_code(),
        })
    return pd.DataFrame(rows)


def save_data(df, name):
    """Save as both CSV and Parquet."""
    csv_path = OUTPUT_DIR / f"{name}.csv"
    parquet_path = OUTPUT_DIR / f"{name}.parquet"

    df.to_csv(csv_path, index=False)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_path, compression="snappy")

    size_mb = csv_path.stat().st_size / (1024 * 1024)
    print(f"  ✓ {name}: {len(df):,} rows, {size_mb:.1f} MB (CSV)")


def main():
    print("=" * 60)
    print("  AWS → GCP Migration: Sample Data Generator")
    print("=" * 60)
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    users = generate_user_pool()

    print("[1/3] Orders")
    orders = generate_orders(users)
    save_data(orders, "orders")

    print("[2/3] Events")
    events = generate_events(users)
    save_data(events, "events")

    print("[3/3] User Segments")
    segments = generate_user_segments(users)
    save_data(segments, "user_segments")

    # Summary
    print()
    print("─" * 60)
    print("  DATA QUALITY ISSUES (intentional for testing):")
    null_users = orders["user_id"].isnull().sum()
    neg_amounts = (orders["amount"] < 0).sum()
    dup_events = events["event_id"].duplicated().sum()
    print(f"  • Orders with null user_id:  {null_users}")
    print(f"  • Orders with negative amt:  {neg_amounts}")
    print(f"  • Duplicate event_ids:       {dup_events}")
    print("─" * 60)
    print(f"\n  Output directory: {OUTPUT_DIR.resolve()}")
    print("  ✅ STAGE 1 COMPLETE — Data generated!\n")


if __name__ == "__main__":
    main()
