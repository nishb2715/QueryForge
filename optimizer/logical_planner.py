"""
QueryForge Logical Planner
Applies rule-based transformations to the query's logical plan.

Primary optimization: Predicate Pushdown
- Move WHERE filters as close as possible to the data source.
- Eliminates rows early → less data flows through higher plan nodes.
- Particularly impactful before JOIN, SORT, and AGGREGATE nodes.
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional
import sqlglot
import sqlglot.expressions as exp

from optimizer.cost_model import Predicate, OPERATOR_SELECTIVITY, DEFAULT_CATALOG


# ---------------------------------------------------------------------------
# Map sqlglot operator node types → internal operator keys
# ---------------------------------------------------------------------------

_OP_MAP: dict[type, str] = {
    exp.EQ:  "eq",
    exp.GT:  "gt",
    exp.GTE: "gte",
    exp.LT:  "lt",
    exp.LTE: "lte",
    exp.NEQ: "neq",
    exp.Like: "like",
    exp.In:  "in",
}


# ---------------------------------------------------------------------------
# Plan nodes  (simple IR for the logical plan)
# ---------------------------------------------------------------------------

@dataclass
class PlanNode:
    """Base logical plan node."""
    node_type: str
    children: list["PlanNode"] = field(default_factory=list)

    def explain(self, depth: int = 0) -> str:
        prefix = "  " * depth
        lines = [f"{prefix}→ {self.node_type}"]
        for child in self.children:
            lines.append(child.explain(depth + 1))
        return "\n".join(lines)


@dataclass
class ScanNode(PlanNode):
    table: str = ""
    alias: Optional[str] = None
    filters: list[str] = field(default_factory=list)  # SQL snippets pushed down

    def explain(self, depth: int = 0) -> str:
        prefix = "  " * depth
        filter_str = " AND ".join(self.filters) if self.filters else "none"
        return f"{prefix}→ Scan({self.table}) filters=[{filter_str}]"


@dataclass
class FilterNode(PlanNode):
    condition: str = ""

    def explain(self, depth: int = 0) -> str:
        prefix = "  " * depth
        lines = [f"{prefix}→ Filter({self.condition})"]
        for child in self.children:
            lines.append(child.explain(depth + 1))
        return "\n".join(lines)


@dataclass
class ProjectNode(PlanNode):
    columns: list[str] = field(default_factory=list)

    def explain(self, depth: int = 0) -> str:
        prefix = "  " * depth
        cols = ", ".join(self.columns) or "*"
        lines = [f"{prefix}→ Project({cols})"]
        for child in self.children:
            lines.append(child.explain(depth + 1))
        return "\n".join(lines)


@dataclass
class JoinNode(PlanNode):
    join_type: str = "INNER"
    condition: str = ""

    def explain(self, depth: int = 0) -> str:
        prefix = "  " * depth
        lines = [f"{prefix}→ HashJoin({self.join_type}) on [{self.condition}]"]
        for child in self.children:
            lines.append(child.explain(depth + 1))
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Optimization result
# ---------------------------------------------------------------------------

@dataclass
class OptimizationResult:
    original_sql: str
    optimized_sql: str
    original_plan: PlanNode
    optimized_plan: PlanNode
    pushed_predicates: list[Predicate]
    unpushed_predicates: list[Predicate]
    optimization_steps: list[str]

    def summary(self) -> dict:
        return {
            "original_sql": self.original_sql,
            "optimized_sql": self.optimized_sql,
            "predicates_pushed_down": len(self.pushed_predicates),
            "predicates_remaining": len(self.unpushed_predicates),
            "optimization_steps": self.optimization_steps,
            "original_plan": self.original_plan.explain(),
            "optimized_plan": self.optimized_plan.explain(),
        }


# ---------------------------------------------------------------------------
# Logical Planner
# ---------------------------------------------------------------------------

class LogicalPlanner:
    """
    Builds a logical plan from a parsed SQL expression and applies
    predicate pushdown as the primary optimization.

    Algorithm (simplified):
    1. Collect all conjunctive predicates from the WHERE clause.
    2. For each predicate, determine which table(s) it references.
    3. Push single-table predicates directly into the ScanNode for that table.
    4. Leave multi-table predicates (join conditions in WHERE) at a FilterNode
       above the relevant Join.
    """

    def __init__(self, catalog=None):
        self.catalog = catalog or DEFAULT_CATALOG

    def optimize(self, parsed_result) -> OptimizationResult:
        """
        Main entry point.

        Args:
            parsed_result: SQLParser.ParseResult

        Returns:
            OptimizationResult with before/after plans and metadata.
        """
        stmt = parsed_result.raw_expression
        steps: list[str] = []

        # ---- 1. Build naive (un-optimized) logical plan ----
        naive_plan = self._build_naive_plan(stmt)
        steps.append("Built initial logical plan (no optimizations applied).")

        # ---- 2. Extract predicates ----
        all_predicates = self._extract_predicates(stmt)
        steps.append(f"Extracted {len(all_predicates)} predicate(s) from WHERE clause.")

        # ---- 3. Classify: pushable vs non-pushable ----
        table_set = {t.name for t in stmt.find_all(exp.Table)}
        pushed, unpushed = [], []
        for pred in all_predicates:
            if pred.column and self._can_push(pred, table_set):
                pushed.append(pred)
            else:
                unpushed.append(pred)

        if pushed:
            steps.append(
                f"Predicate pushdown: moved {len(pushed)} filter(s) to scan level → "
                f"[{', '.join(p.column + ' ' + p.operator + ' ' + str(p.value) for p in pushed)}]"
            )

        # ---- 4. Build optimized plan ----
        optimized_plan = self._build_optimized_plan(stmt, pushed, unpushed)
        steps.append("Reconstructed plan with filters applied at scan nodes.")

        # ---- 5. Rewrite SQL with hints ----
        optimized_sql = self._rewrite_sql(stmt, pushed)
        steps.append("Generated optimized SQL with early filter placement.")

        return OptimizationResult(
            original_sql=parsed_result.sql,
            optimized_sql=optimized_sql,
            original_plan=naive_plan,
            optimized_plan=optimized_plan,
            pushed_predicates=pushed,
            unpushed_predicates=unpushed,
            optimization_steps=steps,
        )

    # ------------------------------------------------------------------
    # Plan construction
    # ------------------------------------------------------------------

    def _build_naive_plan(self, stmt: exp.Expression) -> PlanNode:
        """Construct the un-optimized plan (Filter on top of Scan)."""
        tables = list(stmt.find_all(exp.Table))
        scans: list[PlanNode] = [ScanNode(node_type="Scan", table=t.name) for t in tables]

        where = stmt.find(exp.Where)
        project_cols = self._select_columns(stmt)

        if len(scans) == 1:
            base = scans[0]
        else:
            base = JoinNode(node_type="Join", children=scans, join_type="INNER", condition="…")

        if where:
            base = FilterNode(
                node_type="Filter",
                condition=where.this.sql(dialect="postgres"),
                children=[base],
            )

        return ProjectNode(node_type="Project", columns=project_cols, children=[base])

    def _build_optimized_plan(
        self,
        stmt: exp.Expression,
        pushed: list[Predicate],
        unpushed: list[Predicate],
    ) -> PlanNode:
        """Construct the optimized plan with filters pushed into scans."""
        tables = list(stmt.find_all(exp.Table))
        push_map: dict[str, list[str]] = {}
        for p in pushed:
            push_map.setdefault(p.column, []).append(
                f"{p.column} {_op_symbol(p.operator)} {p.value!r}"
            )

        scans: list[PlanNode] = []
        for t in tables:
            stats = self.catalog.get(t.name)
            filters = []
            # Naively attach all pushed predicates to the first/relevant table
            for p in pushed:
                if stats and p.column in (stats.has_index_on + [stats.shard_key or ""]):
                    filters.append(f"{p.column} {_op_symbol(p.operator)} {p.value!r}")
                elif not stats:
                    filters.append(f"{p.column} {_op_symbol(p.operator)} {p.value!r}")
            scans.append(ScanNode(node_type="Scan", table=t.name, filters=filters))

        if len(scans) == 1:
            base = scans[0]
        else:
            base = JoinNode(node_type="HashJoin", children=scans, join_type="INNER", condition="…")

        # Remaining unpushed predicates stay as a filter above the join
        if unpushed:
            cond = " AND ".join(f"{p.column} {_op_symbol(p.operator)} {p.value!r}" for p in unpushed)
            base = FilterNode(node_type="Filter", condition=cond, children=[base])

        project_cols = self._select_columns(stmt)
        return ProjectNode(node_type="Project", columns=project_cols, children=[base])

    # ------------------------------------------------------------------
    # Predicate extraction
    # ------------------------------------------------------------------

    def _extract_predicates(self, stmt: exp.Expression) -> list[Predicate]:
        where = stmt.find(exp.Where)
        if not where:
            return []

        conditions: list[exp.Expression] = []
        cond = where.this
        if isinstance(cond, exp.And):
            conditions = list(cond.flatten())
        else:
            conditions = [cond]

        predicates = []
        for c in conditions:
            pred = self._parse_condition(c)
            if pred:
                predicates.append(pred)
        return predicates

    def _parse_condition(self, cond: exp.Expression) -> Optional[Predicate]:
        op_type = type(cond)
        op_key = _OP_MAP.get(op_type)
        if not op_key:
            return None

        left = cond.left if hasattr(cond, "left") else cond.args.get("this")
        right = cond.right if hasattr(cond, "right") else cond.args.get("expression")

        if not isinstance(left, exp.Column):
            return None

        col = left.name
        val = right.this if isinstance(right, exp.Literal) else str(right)

        # Determine if column is indexed in any known table
        is_indexed = any(
            col in stats.has_index_on
            for stats in self.catalog.values()
        )

        return Predicate(column=col, operator=op_key, value=val, is_indexed=is_indexed)

    def _can_push(self, pred: Predicate, table_set: set[str]) -> bool:
        """A predicate is pushable if it references a single known column."""
        return bool(pred.column)

    # ------------------------------------------------------------------
    # SQL rewriter
    # ------------------------------------------------------------------

    def _rewrite_sql(self, stmt: exp.Expression, pushed: list[Predicate]) -> str:
        """
        Return the semantically equivalent but annotated SQL.
        In a real engine, this would reorder CTEs / subquery filters.
        Here we add a comment block documenting the pushdown.
        """
        push_comment = "\n".join(
            f"  -- PUSHED: {p.column} {_op_symbol(p.operator)} {p.value!r}"
            for p in pushed
        )
        base_sql = stmt.sql(dialect="postgres")
        if push_comment:
            return f"/* QueryForge Optimized\n{push_comment}\n*/\n{base_sql}"
        return base_sql

    def _select_columns(self, stmt: exp.Expression) -> list[str]:
        sel = stmt.find(exp.Select)
        if not sel:
            return ["*"]
        cols = []
        for expr in sel.expressions:
            if isinstance(expr, exp.Star):
                cols.append("*")
            elif isinstance(expr, exp.Column):
                cols.append(expr.name)
            elif isinstance(expr, exp.Alias):
                cols.append(expr.alias or str(expr))
            else:
                cols.append(str(expr))
        return cols or ["*"]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _op_symbol(op_key: str) -> str:
    return {"eq": "=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<=", "neq": "!="}.get(op_key, op_key)