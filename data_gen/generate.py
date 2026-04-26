#!/usr/bin/env python3
"""
QueryForge Data Generator
Generates a 1,000,000-row synthetic users dataset and splits it
between Shard A (IDs 0–499,999) and Shard B (IDs 500,000–999,999).

Usage:
    python data_gen/generate.py               # generates CSV + SQL init files
    python data_gen/generate.py --rows 100000 # smaller run for testing
"""

import argparse
import csv
import os
import random
import time
from pathlib import Path

# ── deterministic seed so benchmarks are reproducible ──────────────────────
random.seed(42)

CITIES = [
    "Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata",
    "Hyderabad", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
    "Surat", "Kanpur", "Nagpur", "Indore", "Patna",
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun",
    "Ananya", "Diya", "Meera", "Priya", "Riya",
    "Rahul", "Rohit", "Amit", "Sanjay", "Vikram",
    "Kavya", "Ishaan", "Nisha", "Pooja", "Sneha",
]

LAST_NAMES = [
    "Sharma", "Verma", "Singh", "Gupta", "Kumar",
    "Patel", "Joshi", "Mehta", "Agarwal", "Yadav",
]


def generate_row(uid: int) -> tuple:
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    age = random.randint(18, 75)
    city = random.choice(CITIES)
    salary = random.randint(300_000, 5_000_000)  # INR annual
    score = round(random.uniform(0, 100), 2)
    return (uid, name, age, city, salary, score)


def generate_dataset(total_rows: int, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "users.csv"
    shard_a_sql = out_dir / "init_shard_a.sql"
    shard_b_sql = out_dir / "init_shard_b.sql"

    midpoint = total_rows // 2

    print(f"Generating {total_rows:,} rows …")
    start = time.perf_counter()

    # Write CSV (full dataset for naive benchmarks)
    with open(csv_path, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["id", "name", "age", "city", "salary", "score"])
        for uid in range(total_rows):
            writer.writerow(generate_row(uid))
            if uid % 100_000 == 0 and uid > 0:
                elapsed = time.perf_counter() - start
                print(f"  {uid:>7,} rows written … ({elapsed:.1f}s)")

    elapsed = time.perf_counter() - start
    print(f"CSV written to {csv_path}  ({elapsed:.1f}s)")

    # Write Shard A init SQL  (IDs 0 … midpoint-1)
    _write_shard_sql(
        path=shard_a_sql,
        db="shard_a",
        total_rows=total_rows,
        id_range=range(0, midpoint),
    )
    print(f"Shard A SQL written to {shard_a_sql}")

    # Write Shard B init SQL  (IDs midpoint … total_rows-1)
    _write_shard_sql(
        path=shard_b_sql,
        db="shard_b",
        total_rows=total_rows,
        id_range=range(midpoint, total_rows),
    )
    print(f"Shard B SQL written to {shard_b_sql}")
    print(f"\nDone!  Total time: {time.perf_counter() - start:.2f}s")


def _write_shard_sql(path: Path, db: str, total_rows: int, id_range: range):
    random.seed(42)  # Reset for reproducibility

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"-- QueryForge: {db} initialization\n")
        f.write("CREATE TABLE IF NOT EXISTS users (\n")
        f.write("    id      BIGINT PRIMARY KEY,\n")
        f.write("    name    VARCHAR(100) NOT NULL,\n")
        f.write("    age     SMALLINT     NOT NULL,\n")
        f.write("    city    VARCHAR(60)  NOT NULL,\n")
        f.write("    salary  INTEGER      NOT NULL,\n")
        f.write("    score   NUMERIC(5,2) NOT NULL\n")
        f.write(");\n\n")
        f.write("CREATE INDEX IF NOT EXISTS idx_users_city   ON users(city);\n")
        f.write("CREATE INDEX IF NOT EXISTS idx_users_age    ON users(age);\n")
        f.write("CREATE INDEX IF NOT EXISTS idx_users_salary ON users(salary);\n\n")

        # Advance the random state to match CSV generation
        for uid in range(0, id_range.start):
            generate_row(uid)  # discard — advance RNG

        # Use COPY (tab-separated) — no batching, no INSERT syntax issues
        f.write("COPY users (id, name, age, city, salary, score) FROM stdin WITH (FORMAT text, DELIMITER E'\\t');\n")
        for uid in id_range:
            row = generate_row(uid)
            f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\n")
        f.write("\\.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="QueryForge dataset generator")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Total rows to generate")
    parser.add_argument("--out", type=str, default="data_gen", help="Output directory")
    args = parser.parse_args()

    generate_dataset(args.rows, Path(args.out))