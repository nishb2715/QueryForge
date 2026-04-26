"""
QueryForge AST Parser
Converts raw SQL strings into structured Abstract Syntax Trees using sqlglot.
"""

from __future__ import annotations
import json
from dataclasses import dataclass, field, asdict
from typing import Any
import sqlglot
import sqlglot.expressions as exp


@dataclass
class ASTNode:
    """Lightweight, serializable AST node."""
    node_type: str
    value: Any = None
    children: list["ASTNode"] = field(default_factory=list)
    meta: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        lines = [f"{prefix}{self.node_type}" + (f": {self.value!r}" if self.value else "")]
        for child in self.children:
            lines.append(child.pretty(indent + 1))
        return "\n".join(lines)


def _convert(node: exp.Expression) -> ASTNode:
    """Recursively convert a sqlglot expression into an ASTNode."""
    if node is None:
        return ASTNode("NULL")

    node_type = type(node).__name__
    value = None
    meta: dict = {}

    if isinstance(node, exp.Literal):
        value = node.this
        meta["is_string"] = node.is_string
    elif isinstance(node, exp.Column):
        value = node.name
        if node.table:
            meta["table"] = node.table
    elif isinstance(node, exp.Table):
        value = node.name
        if node.alias:
            meta["alias"] = node.alias
    elif isinstance(node, (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE, exp.NEQ)):
        meta["operator"] = node.key
    elif isinstance(node, exp.Anonymous):
        value = node.name

    children = [_convert(child) for child in node.args.values() if isinstance(child, exp.Expression)]

    return ASTNode(node_type=node_type, value=value, children=children, meta=meta)


class SQLParser:
    """
    Parses a SQL string into a QueryForge ASTNode tree.

    Usage:
        parser = SQLParser()
        result = parser.parse("SELECT id, name FROM users WHERE age > 25")
        print(result.ast.pretty())
    """

    @dataclass
    class ParseResult:
        sql: str
        dialect: str
        ast: ASTNode
        raw_expression: exp.Expression

        @property
        def tables(self) -> list[str]:
            return [t.name for t in self.raw_expression.find_all(exp.Table)]

        @property
        def columns(self) -> list[str]:
            return [c.name for c in self.raw_expression.find_all(exp.Column)]

        @property
        def where_conditions(self) -> list[exp.Expression]:
            where = self.raw_expression.find(exp.Where)
            if not where:
                return []
            cond = where.this
            if isinstance(cond, exp.And):
                return list(cond.flatten())
            return [cond]

    def parse(self, sql: str, dialect: str = "postgres") -> "SQLParser.ParseResult":
        """
        Parse a SQL string and return a ParseResult.

        Args:
            sql:     Raw SQL query string.
            dialect: SQL dialect for sqlglot (default: "postgres").

        Returns:
            ParseResult with AST and metadata.

        Raises:
            ValueError: If the SQL cannot be parsed.
        """
        try:
            expressions = sqlglot.parse(sql, dialect=dialect)
        except Exception as exc:
            raise ValueError(f"SQL parse error: {exc}") from exc

        if not expressions:
            raise ValueError("No SQL statements found.")

        raw_expr = expressions[0]
        ast_root = _convert(raw_expr)

        return self.ParseResult(
            sql=sql,
            dialect=dialect,
            ast=ast_root,
            raw_expression=raw_expr,
        )

    def to_json(self, result: "SQLParser.ParseResult", indent: int = 2) -> str:
        return json.dumps(result.ast.to_dict(), indent=indent)