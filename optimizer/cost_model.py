"""
QueryForge Cost-Based Optimizer (CBO)
Estimates query cost in terms of rows scanned and I/O units.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import math


# ---------------------------------------------------------------------------
# Table Statistics (mock catalog — in production this comes from pg_statistic)
# ---------------------------------------------------------------------------

@dataclass
class TableStats:
    table_name: str
    total_rows: int
    avg_row_bytes: int = 200          # average row size in bytes
    pages: int = 0                    # data pages (auto-computed if 0)
    has_index_on: list[str] = field(default_factory=list)
    shard_key: Optional[str] = None   # column used for range sharding
    shard_ranges: dict = field(default_factory=dict)  # {"shard_a": (0,499999), "shard_b": (500000, 999999)}

    def __post_init__(self):
        if self.pages == 0:
            self.pages = max(1, (self.total_rows * self.avg_row_bytes) // (8 * 1024))


# Default catalog used by the optimizer when no external stats are provided.
DEFAULT_CATALOG: dict[str, TableStats] = {
    "users": TableStats(
        table_name="users",
        total_rows=1_000_000,
        avg_row_bytes=200,
        has_index_on=["id", "city", "age"],
        shard_key="id",
        shard_ranges={
            "shard_a": (0, 499_999),
            "shard_b": (500_000, 999_999),
        },
    ),
    "orders": TableStats(
        table_name="orders",
        total_rows=5_000_000,
        avg_row_bytes=150,
        has_index_on=["id", "user_id"],
        shard_key="user_id",
        shard_ranges={
            "shard_a": (0, 499_999),
            "shard_b": (500_000, 999_999),
        },
    ),
}


# ---------------------------------------------------------------------------
# Predicate selectivity estimation
# ---------------------------------------------------------------------------

OPERATOR_SELECTIVITY = {
    "eq": 0.01,   # col = val  → ~1% of rows
    "gt": 0.33,
    "gte": 0.34,
    "lt": 0.33,
    "lte": 0.34,
    "neq": 0.99,
    "like": 0.05,
    "in": 0.10,
}


@dataclass
class Predicate:
    column: str
    operator: str   # one of OPERATOR_SELECTIVITY keys
    value: str
    is_indexed: bool = False


def combined_selectivity(predicates: list[Predicate]) -> float:
    """
    Estimate combined selectivity assuming column independence
    (standard CBO assumption).  Returns a float in (0, 1].
    """
    sel = 1.0
    for p in predicates:
        sel *= OPERATOR_SELECTIVITY.get(p.operator, 0.5)
    return max(sel, 1e-6)


# ---------------------------------------------------------------------------
# Cost model
# ---------------------------------------------------------------------------

@dataclass
class ScanCost:
    plan_type: str          # "FullTableScan" | "IndexScan" | "ShardedScan"
    rows_scanned: int
    estimated_io_cost: float
    estimated_cpu_cost: float
    total_cost: float
    shards_accessed: list[str] = field(default_factory=list)
    notes: str = ""

    @property
    def summary(self) -> str:
        shards = f" ({', '.join(self.shards_accessed)})" if self.shards_accessed else ""
        return (
            f"{self.plan_type}{shards} | rows={self.rows_scanned:,} | "
            f"io={self.estimated_io_cost:.1f} | cpu={self.estimated_cpu_cost:.1f} | "
            f"total={self.total_cost:.1f}"
        )


class CostModel:
    """
    Estimates the cost of executing a query given table statistics and predicates.

    Cost formula (simplified Selinger-style):
        io_cost  = pages_read * SEQ_PAGE_COST  (or * RANDOM_PAGE_COST for index)
        cpu_cost = rows_scanned * CPU_TUPLE_COST
        total    = io_cost + cpu_cost

    Constants mirror PostgreSQL defaults.
    """

    SEQ_PAGE_COST = 1.0       # cost per sequential page read
    RANDOM_PAGE_COST = 4.0    # cost per random page read (index)
    CPU_TUPLE_COST = 0.01     # cost per row processed

    def __init__(self, catalog: dict[str, TableStats] | None = None):
        self.catalog = catalog or DEFAULT_CATALOG

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def estimate(
        self,
        table: str,
        predicates: list[Predicate],
        sharding_predicate_column: Optional[str] = None,
        sharding_predicate_value: Optional[int] = None,
    ) -> tuple[ScanCost, ScanCost]:
        """
        Returns (naive_cost, optimized_cost) for comparison.

        Args:
            table:                      Table name (looked up in catalog).
            predicates:                 List of WHERE predicates.
            sharding_predicate_column:  Column used in a shard-routing filter.
            sharding_predicate_value:   Concrete value for shard routing.

        Returns:
            Tuple of (NaiveCost, OptimizedCost).
        """
        stats = self._get_stats(table)
        naive = self._full_table_scan(stats)
        optimized = self._best_plan(stats, predicates, sharding_predicate_column, sharding_predicate_value)
        return naive, optimized

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_stats(self, table: str) -> TableStats:
        if table not in self.catalog:
            # Unknown table: use conservative defaults
            return TableStats(table_name=table, total_rows=100_000)
        return self.catalog[table]

    def _full_table_scan(self, stats: TableStats) -> ScanCost:
        io = stats.pages * self.SEQ_PAGE_COST
        cpu = stats.total_rows * self.CPU_TUPLE_COST
        return ScanCost(
            plan_type="FullTableScan",
            rows_scanned=stats.total_rows,
            estimated_io_cost=io,
            estimated_cpu_cost=cpu,
            total_cost=io + cpu,
            notes="No predicates applied; scans entire table.",
        )

    def _best_plan(
        self,
        stats: TableStats,
        predicates: list[Predicate],
        shard_col: Optional[str],
        shard_val: Optional[int],
    ) -> ScanCost:

        sel = combined_selectivity(predicates) if predicates else 1.0
        rows_after_filter = max(1, int(stats.total_rows * sel))

        # --- Determine shards to hit ---
        active_shards = self._route_shards(stats, shard_col, shard_val)
        shard_fraction = len(active_shards) / max(len(stats.shard_ranges), 1) if stats.shard_ranges else 1.0

        # Effective rows = fraction of shards × selectivity
        effective_rows = max(1, int(rows_after_filter * shard_fraction))

        # --- Choose index vs seq scan ---
        indexed_cols = {p.column for p in predicates if p.is_indexed}
        use_index = bool(indexed_cols)

        if active_shards and len(active_shards) < len(stats.shard_ranges):
            plan_type = "ShardedScan"
            effective_pages = max(1, int(stats.pages * shard_fraction * sel))
            io = effective_pages * (self.RANDOM_PAGE_COST if use_index else self.SEQ_PAGE_COST)
            notes = f"Shard pruning eliminated {len(stats.shard_ranges) - len(active_shards)} shard(s). "
        elif use_index:
            plan_type = "IndexScan"
            effective_pages = max(1, int(math.log2(stats.pages + 1) + effective_rows * 0.1))
            io = effective_pages * self.RANDOM_PAGE_COST
            notes = f"Index used on: {', '.join(indexed_cols)}. "
        else:
            plan_type = "FilteredSeqScan"
            effective_pages = max(1, int(stats.pages * sel))
            io = effective_pages * self.SEQ_PAGE_COST
            notes = "No usable index; sequential scan with early filter. "

        notes += f"Selectivity={sel:.4f}, rows_scanned={effective_rows:,}."

        cpu = effective_rows * self.CPU_TUPLE_COST
        return ScanCost(
            plan_type=plan_type,
            rows_scanned=effective_rows,
            estimated_io_cost=round(io, 2),
            estimated_cpu_cost=round(cpu, 4),
            total_cost=round(io + cpu, 2),
            shards_accessed=active_shards,
            notes=notes,
        )

    def _route_shards(
        self,
        stats: TableStats,
        col: Optional[str],
        val: Optional[int],
    ) -> list[str]:
        """Determine which shards need to be queried."""
        if not stats.shard_ranges or col != stats.shard_key or val is None:
            return list(stats.shard_ranges.keys()) or []

        result = []
        for shard_name, (lo, hi) in stats.shard_ranges.items():
            if lo <= val <= hi:
                result.append(shard_name)
        return result or list(stats.shard_ranges.keys())