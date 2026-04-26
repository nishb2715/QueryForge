"""
QueryForge Physical Planner
Translates a logical plan into a concrete physical execution plan
by selecting the cheapest scan strategy using the CostModel.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional

from optimizer.cost_model import CostModel, ScanCost, Predicate, DEFAULT_CATALOG
from optimizer.logical_planner import OptimizationResult


@dataclass
class PhysicalScan:
    table: str
    strategy: str          # "FullTableScan" | "IndexScan" | "ShardedScan"
    shards: list[str]
    naive_cost: ScanCost
    optimized_cost: ScanCost
    speedup_factor: float

    def to_dict(self) -> dict:
        return {
            "table": self.table,
            "strategy": self.strategy,
            "shards_accessed": self.shards,
            "naive_cost": {
                "plan_type": self.naive_cost.plan_type,
                "rows_scanned": self.naive_cost.rows_scanned,
                "total_cost": self.naive_cost.total_cost,
            },
            "optimized_cost": {
                "plan_type": self.optimized_cost.plan_type,
                "rows_scanned": self.optimized_cost.rows_scanned,
                "total_cost": self.optimized_cost.total_cost,
                "notes": self.optimized_cost.notes,
            },
            "speedup_factor": round(self.speedup_factor, 2),
        }


@dataclass
class PhysicalPlan:
    scans: list[PhysicalScan]
    join_strategy: Optional[str] = None   # "HashJoin" | "NestedLoop" | "MergeJoin"
    join_notes: str = ""
    total_naive_cost: float = 0.0
    total_optimized_cost: float = 0.0

    @property
    def overall_speedup(self) -> float:
        if self.total_optimized_cost == 0:
            return 1.0
        return round(self.total_naive_cost / self.total_optimized_cost, 2)

    def to_dict(self) -> dict:
        return {
            "scans": [s.to_dict() for s in self.scans],
            "join_strategy": self.join_strategy,
            "join_notes": self.join_notes,
            "total_naive_cost": round(self.total_naive_cost, 2),
            "total_optimized_cost": round(self.total_optimized_cost, 2),
            "overall_speedup_factor": self.overall_speedup,
        }


class PhysicalPlanner:
    """
    Converts an OptimizationResult into a PhysicalPlan.

    Join Strategy Selection (Hash Join vs Nested Loop vs Merge Join):
    ──────────────────────────────────────────────────────────────────
    • Hash Join  → Best when one side is small enough to fit in memory
                   (e.g. large table × lookup table).  O(n+m).
    • Nested Loop → Best for small outer tables + indexed inner table.
                    O(n × m) worst case, but index access makes it fast.
    • Merge Join  → Best when both sides are pre-sorted on the join key.
                    O(n log n + m log m).

    This planner picks HashJoin for cross-table queries when the smaller
    table is < 10% the size of the larger — matching PostgreSQL's heuristic.
    """

    HASH_JOIN_RATIO_THRESHOLD = 0.10  # use hash join when small/large < 10%

    def __init__(self, catalog=None):
        self.catalog = catalog or DEFAULT_CATALOG
        self.cost_model = CostModel(catalog=self.catalog)

    def plan(
        self,
        opt_result: OptimizationResult,
        tables: list[str],
        shard_col: Optional[str] = None,
        shard_val: Optional[int] = None,
    ) -> PhysicalPlan:
        """
        Produce a PhysicalPlan from an OptimizationResult.

        Args:
            opt_result:  Output from LogicalPlanner.optimize().
            tables:      List of table names in the query.
            shard_col:   Column used for shard routing (if any).
            shard_val:   Value used for shard routing (if any).

        Returns:
            PhysicalPlan with per-table scan strategies and costs.
        """
        predicates = opt_result.pushed_predicates + opt_result.unpushed_predicates
        scans: list[PhysicalScan] = []
        total_naive = 0.0
        total_opt = 0.0

        for table in tables:
            naive_cost, opt_cost = self.cost_model.estimate(
                table=table,
                predicates=predicates,
                sharding_predicate_column=shard_col,
                sharding_predicate_value=shard_val,
            )
            speedup = naive_cost.total_cost / opt_cost.total_cost if opt_cost.total_cost else 1.0
            scans.append(PhysicalScan(
                table=table,
                strategy=opt_cost.plan_type,
                shards=opt_cost.shards_accessed,
                naive_cost=naive_cost,
                optimized_cost=opt_cost,
                speedup_factor=speedup,
            ))
            total_naive += naive_cost.total_cost
            total_opt += opt_cost.total_cost

        # --- Join strategy (only relevant for multi-table queries) ---
        join_strategy = None
        join_notes = ""
        if len(tables) > 1:
            join_strategy, join_notes = self._select_join_strategy(tables)

        return PhysicalPlan(
            scans=scans,
            join_strategy=join_strategy,
            join_notes=join_notes,
            total_naive_cost=round(total_naive, 2),
            total_optimized_cost=round(total_opt, 2),
        )

    # ------------------------------------------------------------------
    # Join strategy selection
    # ------------------------------------------------------------------

    def _select_join_strategy(self, tables: list[str]) -> tuple[str, str]:
        sizes = {t: self.catalog[t].total_rows if t in self.catalog else 100_000 for t in tables}
        sorted_tables = sorted(sizes.items(), key=lambda x: x[1])
        smallest_name, smallest_rows = sorted_tables[0]
        largest_name, largest_rows = sorted_tables[-1]
        ratio = smallest_rows / largest_rows if largest_rows else 1.0

        if ratio < self.HASH_JOIN_RATIO_THRESHOLD:
            return (
                "HashJoin",
                f"Build hash table on '{smallest_name}' ({smallest_rows:,} rows), "
                f"probe with '{largest_name}' ({largest_rows:,} rows). "
                f"Ratio={ratio:.3f} < threshold={self.HASH_JOIN_RATIO_THRESHOLD}. "
                "HashJoin chosen: O(n+m) vs O(n×m) nested loop.",
            )
        else:
            return (
                "MergeJoin",
                f"Tables are similarly sized ({smallest_rows:,} vs {largest_rows:,}). "
                "MergeJoin chosen if both sides sorted on join key; else NestedLoop.",
            )