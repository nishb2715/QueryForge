#!/usr/bin/env python3
"""
QueryForge Demo
Shows the full optimization pipeline for a sample SQL query:
  1. Parse SQL → AST
  2. Extract predicates
  3. Apply predicate pushdown (logical planning)
  4. Select physical scan strategy (cost model)
  5. Print before/after plans

Run: python demo.py
"""

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.tree import Tree
from rich import box
from rich.table import Table
import json

from optimizer.ast_parser import SQLParser
from optimizer.logical_planner import LogicalPlanner
from optimizer.physical_planner import PhysicalPlanner

console = Console()

SAMPLE_SQL = "SELECT id, name, age, city FROM users WHERE age > 25 AND city = 'Delhi'"

JOIN_SQL = (
    "SELECT u.name, o.amount FROM users u "
    "JOIN orders o ON u.id = o.user_id "
    "WHERE u.age > 30 AND o.amount > 5000"
)


def demo_single_table():
    console.print(Panel.fit(
        "[bold cyan]QueryForge — Single-Table Optimization Demo[/bold cyan]",
        border_style="cyan"
    ))

    # ── Step 1: Parse ─────────────────────────────────────────────────────
    console.print("\n[bold yellow]Step 1: SQL → Abstract Syntax Tree (AST)[/bold yellow]")
    sql = Syntax(SAMPLE_SQL, "sql", theme="monokai", line_numbers=False)
    console.print(sql)

    parser = SQLParser()
    parsed = parser.parse(SAMPLE_SQL)

    console.print("\n[dim]AST (truncated top-level nodes):[/dim]")
    ast_dict = parsed.ast.to_dict()
    console.print_json(json.dumps({"node_type": ast_dict["node_type"],
                                   "children_count": len(ast_dict["children"])}, indent=2))

    console.print(f"\n  Tables  : {parsed.tables}")
    console.print(f"  Columns : {parsed.columns}")

    # ── Step 2: Logical Plan (Before) ────────────────────────────────────
    console.print("\n[bold yellow]Step 2: Naive Logical Plan (no optimizations)[/bold yellow]")
    planner = LogicalPlanner()
    opt = planner.optimize(parsed)

    console.print(Panel(opt.original_plan.explain(), title="BEFORE (Naive Plan)", border_style="red"))

    # ── Step 3: Apply Predicate Pushdown ─────────────────────────────────
    console.print("\n[bold yellow]Step 3: Applying Predicate Pushdown[/bold yellow]")
    for step in opt.optimization_steps:
        console.print(f"  ✓ {step}")

    console.print(Panel(opt.optimized_plan.explain(), title="AFTER (Optimized Plan)", border_style="green"))

    # ── Step 4: Physical Plan ────────────────────────────────────────────
    console.print("\n[bold yellow]Step 4: Physical Plan — Cost Model Decision[/bold yellow]")
    physical = PhysicalPlanner()
    phys = physical.plan(opt, tables=["users"])

    t = Table(box=box.SIMPLE_HEAD)
    t.add_column("Metric")
    t.add_column("Naive", style="red")
    t.add_column("Optimized", style="green")

    scan = phys.scans[0]
    t.add_row("Plan Type",      scan.naive_cost.plan_type,      scan.optimized_cost.plan_type)
    t.add_row("Rows Scanned",   f"{scan.naive_cost.rows_scanned:,}", f"{scan.optimized_cost.rows_scanned:,}")
    t.add_row("I/O Cost",       f"{scan.naive_cost.estimated_io_cost:.1f}", f"{scan.optimized_cost.estimated_io_cost:.1f}")
    t.add_row("Total Cost",     f"{scan.naive_cost.total_cost:.1f}", f"{scan.optimized_cost.total_cost:.1f}")
    t.add_row("Speedup Factor", "1.0×",                          f"[bold]{scan.speedup_factor:.1f}×[/bold]")

    console.print(t)
    console.print(f"\n  [dim]Notes: {scan.optimized_cost.notes}[/dim]")

    # ── Step 5: Generated SQL ────────────────────────────────────────────
    console.print("\n[bold yellow]Step 5: Optimized SQL Output[/bold yellow]")
    console.print(Syntax(opt.optimized_sql, "sql", theme="monokai"))


def demo_join():
    console.print(Panel.fit(
        "[bold cyan]QueryForge — JOIN Optimization Demo (Hash Join)[/bold cyan]",
        border_style="cyan"
    ))

    console.print("\n[bold yellow]Input SQL with JOIN:[/bold yellow]")
    console.print(Syntax(JOIN_SQL, "sql", theme="monokai"))

    parser = SQLParser()
    parsed = parser.parse(JOIN_SQL)

    planner = LogicalPlanner()
    opt = planner.optimize(parsed)

    physical = PhysicalPlanner()
    phys = physical.plan(opt, tables=["users", "orders"])

    console.print(Panel(opt.optimized_plan.explain(), title="Optimized Plan (JOIN)", border_style="green"))

    if phys.join_strategy:
        console.print(f"\n[bold]Join Strategy Selected: [cyan]{phys.join_strategy}[/cyan][/bold]")
        console.print(f"  {phys.join_notes}")

    console.print(f"\n  Combined Speedup: [bold green]{phys.overall_speedup}×[/bold green]")


if __name__ == "__main__":
    demo_single_table()
    console.print("\n" + "─" * 70 + "\n")
    demo_join()
    console.print()