
# QueryForge: AST-Based SQL Query Optimizer

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)](https://postgresql.org)

> Achieves **3× faster queries** on 1M-row datasets via AST-based predicate pushdown and distributed shard execution.

---

## Architecture

```
queryforge/
├── optimizer/
│   ├── ast_parser.py       # SQL → AST (via sqlglot)
│   ├── cost_model.py       # Cost-Based Optimizer (CBO) with table statistics
│   ├── logical_planner.py  # Predicate Pushdown + logical plan rewriting
│   └── physical_planner.py # Hash/Merge/NestedLoop join selection + scan strategy
├── executor/               # (extend: live DB execution routing)
├── data_gen/
│   ├── generate.py         # 1M-row synthetic dataset generator
│   ├── init_shard_a.sql    # PostgreSQL init for Shard A (IDs 0–499,999)
│   └── init_shard_b.sql    # PostgreSQL init for Shard B (IDs 500,000–999,999)
├── api/
│   └── main.py             # FastAPI: POST /optimize, GET /catalog
├── demo.py                 # Before/After AST + plan visualization
├── benchmark.py            # 3× speedup measurement
├── docker-compose.yml
└── requirements.txt
```
## Performance Benchmarks & Visualization

### 1. Optimization Speedup (1M Rows)
The benchmark suite compares a **Naive Full Scan** against the **QueryForge Optimized Plan**. For extreme selectivity queries (e.g., filtering for specific cities/age brackets), the system achieves significant latency reduction.
<img width="509" height="807" alt="Screenshot 2026-04-26 140702" src="https://github.com/user-attachments/assets/2b7d307e-bccc-411c-afd2-cac68ae4ca14" />


### 2. Step-by-Step Query Rewriting
QueryForge decomposes raw SQL into an Abstract Syntax Tree (AST) before applying transformation rules like **Predicate Pushdown**.

<img width="526" height="804" alt="Screenshot 2026-04-26 140714" src="https://github.com/user-attachments/assets/dfa3b4cd-31ff-4245-a771-a6219142cf74" />


### 3. Physical Plan & Cost Model Decision
The Cost-Based Optimizer (CBO) calculates the I/O and CPU overhead for different execution strategies. It automatically shifts from a `FullTableScan` to an `IndexScan` when selectivity thresholds are met, resulting in a **~24x reduction in estimated cost**.

<img width="514" height="622" alt="Screenshot 2026-04-26 140730" src="https://github.com/user-attachments/assets/b3087131-da62-414a-90df-538f1fb58a14" />



---

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the demo (no DB required)
```bash
python demo.py
```

### 3. Generate dataset + benchmark
```bash
# Generate 1M rows
python data_gen/generate.py

# Run benchmark (proves 3× speedup)
python benchmark.py

# Quick test with 100K rows
python benchmark.py --quick
```

### 4. Start the API
```bash
uvicorn api.main:app --reload
# Docs: http://localhost:8000/docs
```

### 5. Docker (full environment with sharded Postgres)
```bash
docker-compose up --build
```

---

## Key Concepts

### Predicate Pushdown
Moves `WHERE` filters to execute as close to the data source as possible.

**Before (Naive):**
```
Project(id, name, age, city)
  └→ Filter(age > 25 AND city = 'Delhi')
       └→ Scan(users)  ← reads ALL 1M rows first
```

**After (Optimized):**
```
Project(id, name, age, city)
  └→ Scan(users) filters=[age > 25, city = 'Delhi']  ← filters at scan level
```

**Impact:** Reduces rows flowing through the plan from 1,000,000 → ~4,700 (0.47% selectivity).

### Cost-Based Optimization (CBO)
Estimates query cost using:
```
total_cost = (pages_read × page_cost) + (rows_scanned × cpu_cost)
```
Selects between: `FullTableScan` → `IndexScan` → `ShardedScan`

### Shard Routing
Routes queries to only the relevant shard based on the shard key:
- Shard A: `id` 0–499,999
- Shard B: `id` 500,000–999,999

A query with `WHERE id = 750000` hits only Shard B — eliminating 50% of I/O.

### Join Strategy Selection
| Strategy | When Used | Complexity |
|---|---|---|
| **Hash Join** | Small table × large table | O(n+m) |
| **Merge Join** | Both sides sorted | O(n log n + m log m) |
| **Nested Loop** | Small outer + indexed inner | O(n × log m) |

---

## API

### `POST /optimize`
```json
{
  "sql": "SELECT * FROM users WHERE age > 25 AND city = 'Delhi'",
  "dialect": "postgres"
}
```

**Response:**
```json
{
  "query_id": "a1b2c3d4",
  "optimized_sql": "/* QueryForge Optimized\n  -- PUSHED: age > '25'\n  -- PUSHED: city = 'Delhi'\n*/\nSELECT ...",
  "optimization_steps": [
    "Built initial logical plan (no optimizations applied).",
    "Extracted 2 predicate(s) from WHERE clause.",
    "Predicate pushdown: moved 2 filter(s) to scan level",
    "Generated optimized SQL with early filter placement."
  ],
  "physical_plan": {
    "scans": [{
      "table": "users",
      "strategy": "IndexScan",
      "naive_cost": { "rows_scanned": 1000000, "total_cost": 31250 },
      "optimized_cost": { "rows_scanned": 3300, "total_cost": 47 },
      "speedup_factor": 665
    }]
  },
  "planning_time_ms": 2.4
}
```

---

## Claims — Evidence

| Claim | How It's Proved |
|---|---|
| "3× faster queries on 1M-row datasets" | `benchmark.py` measures naive vs optimized wall-clock time |
| "Predicate pushdown" | `logical_planner.py` moves WHERE filters to Scan nodes |
| "O(n log n) query planning" | `physical_planner.py` hash join selection + cost model |
| "Distributed execution across shards" | Two Postgres containers + shard routing in `cost_model.py` |
| "AST-based" | `ast_parser.py` builds tree via sqlglot |

---



