"""
QueryForge FastAPI Application
Exposes the optimizer engine as a REST API.
"""

from __future__ import annotations
import time
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from optimizer.ast_parser import SQLParser
from optimizer.logical_planner import LogicalPlanner
from optimizer.physical_planner import PhysicalPlanner
from optimizer.cost_model import DEFAULT_CATALOG

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="QueryForge API",
    description="AST-Based SQL Query Optimizer with Cost-Based Optimization and Predicate Pushdown",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Shared optimizer instances (stateless — safe to reuse)
_parser = SQLParser()
_logical_planner = LogicalPlanner()
_physical_planner = PhysicalPlanner()


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class OptimizeRequest(BaseModel):
    sql: str = Field(
        ...,
        example="SELECT * FROM users WHERE age > 25 AND city = 'Delhi'",
        description="Raw SQL query to optimize.",
    )
    dialect: str = Field(default="postgres", description="SQL dialect.")
    shard_column: Optional[str] = Field(default=None, description="Column to use for shard routing.")
    shard_value: Optional[int] = Field(default=None, description="Value for shard routing.")


class OptimizeResponse(BaseModel):
    query_id: str
    input_sql: str
    optimized_sql: str
    optimization_steps: list[str]
    original_plan: str
    optimized_plan: str
    predicates_pushed: int
    physical_plan: dict
    planning_time_ms: float


class HealthResponse(BaseModel):
    status: str
    version: str
    catalog_tables: list[str]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["System"])
def health():
    """Check API health and available tables in the optimizer catalog."""
    return HealthResponse(
        status="ok",
        version="1.0.0",
        catalog_tables=list(DEFAULT_CATALOG.keys()),
    )


@app.post("/optimize", response_model=OptimizeResponse, tags=["Optimizer"])
def optimize(req: OptimizeRequest):
    """
    Parse and optimize a SQL query.

    Returns:
    - The optimized SQL (with predicate pushdown applied).
    - A human-readable logical plan (before and after).
    - A physical execution plan with cost estimates and speedup factors.
    - A list of optimization steps taken.
    """
    t0 = time.perf_counter()

    # 1. Parse
    try:
        parsed = _parser.parse(req.sql, dialect=req.dialect)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"SQL parse error: {e}")

    # 2. Logical optimization (predicate pushdown)
    opt_result = _logical_planner.optimize(parsed)

    # 3. Physical planning (scan strategy + cost model)
    tables = parsed.tables or ["users"]
    phys_plan = _physical_planner.plan(
        opt_result,
        tables=tables,
        shard_col=req.shard_column,
        shard_val=req.shard_value,
    )

    planning_ms = (time.perf_counter() - t0) * 1000

    import hashlib, json
    query_id = hashlib.md5(req.sql.encode()).hexdigest()[:8]

    return OptimizeResponse(
        query_id=query_id,
        input_sql=req.sql,
        optimized_sql=opt_result.optimized_sql,
        optimization_steps=opt_result.optimization_steps,
        original_plan=opt_result.original_plan.explain(),
        optimized_plan=opt_result.optimized_plan.explain(),
        predicates_pushed=len(opt_result.pushed_predicates),
        physical_plan=phys_plan.to_dict(),
        planning_time_ms=round(planning_ms, 3),
    )


@app.get("/catalog", tags=["Optimizer"])
def catalog():
    """Return the optimizer's table statistics catalog."""
    return {
        name: {
            "total_rows": stats.total_rows,
            "pages": stats.pages,
            "has_index_on": stats.has_index_on,
            "shard_key": stats.shard_key,
            "shard_ranges": stats.shard_ranges,
        }
        for name, stats in DEFAULT_CATALOG.items()
    }


@app.get("/", tags=["System"])
def root():
    return {
        "project": "QueryForge",
        "description": "AST-Based SQL Query Optimizer",
        "endpoints": {
            "POST /optimize": "Optimize a SQL query",
            "GET /catalog": "View table statistics",
            "GET /health": "Health check",
            "GET /docs": "Swagger UI",
        },
    }