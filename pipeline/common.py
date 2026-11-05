"""Small helpers shared by every pipeline stage: paths, config, a laptop-safe DuckDB."""
from pathlib import Path

import duckdb
import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(name: str) -> dict:
    """Read config/<name>.yaml."""
    with open(ROOT / "config" / f"{name}.yaml") as f:
        return yaml.safe_load(f)


def connect(memory_limit: str | None = None, threads: int | None = None,
            temp_directory: str | Path | None = None) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with the memory/thread caps from pipeline.yaml.

    Arguments override pipeline.yaml (the orchestrator gives each parallel worker its own
    caps and temp folder, so workers never share DuckDB spill files).
    """
    cfg = load_config("pipeline")["duckdb"]
    tmp = Path(temp_directory) if temp_directory else ROOT / cfg["temp_directory"]
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"SET memory_limit = '{memory_limit or cfg['memory_limit']}'")
    con.sql(f"SET threads = {int(threads or cfg['threads'])}")
    con.sql(f"SET temp_directory = '{tmp}'")
    con.sql("SET preserve_insertion_order = false")
    return con


def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    """Run a query that returns one value (DuckDB 2.0: relations have no fetchone)."""
    return con.sql(sql).fetchall()[0][0]


def match_sql(column: str, terms: list[str]) -> str:
    """SQL expression: TRUE if `column` contains any term at the START of a word.

    "cisco" matches "Cisco Systems" but not "sanfrancisco.gov". Case-insensitive.
    Word boundaries only work for Latin letters/digits, so terms starting with anything
    else (e.g. Chinese) match anywhere. Empty list -> FALSE. NULL column -> FALSE.
    """
    import re

    if not terms:
        return "FALSE"
    parts = [(r"\b" if re.match(r"[A-Za-z0-9]", t) else "") + re.escape(t) for t in terms]
    pattern = "(?i)(" + "|".join(parts) + ")"
    return f"coalesce(regexp_matches({column}, {sql_str(pattern)}), FALSE)"


def load_env(path: Path = ROOT / ".env") -> None:
    """Read KEY=value lines from .env into os.environ (offline LLM step only).

    A variable already set in the shell wins over .env. Missing file = no-op.
    .env is git-ignored; .env.example lists the keys. The hosted app needs no keys.
    """
    import os

    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def sql_str(s: str) -> str:
    """Quote a Python string as a SQL literal."""
    return "'" + s.replace("'", "''") + "'"
