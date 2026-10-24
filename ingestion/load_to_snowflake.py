"""Load CSV files into one raw Snowflake table per file."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any


def normalize_identifier(value: str) -> str:
    """Convert an input name to a safe, unquoted Snowflake identifier."""
    identifier = re.sub(r"[^A-Za-z0-9_]", "_", value).upper()
    if not identifier:
        identifier = "COLUMN"
    if not re.match(r"[A-Z_]", identifier):
        identifier = f"T_{identifier}"
    return identifier


def csv_columns(csv_path: Path) -> list[str]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as source:
        try:
            header = next(csv.reader(source))
        except StopIteration as exc:
            raise ValueError(f"{csv_path} is empty; a header row is required") from exc

    if not header or not any(column.strip() for column in header):
        raise ValueError(f"{csv_path} has no usable column names")

    columns: list[str] = []
    used: set[str] = set()
    for index, name in enumerate(header, start=1):
        base = normalize_identifier(name.strip()) if name.strip() else f"COLUMN_{index}"
        column = base
        suffix = 2
        while column in used:
            column = f"{base}_{suffix}"
            suffix += 1
        columns.append(column)
        used.add(column)
    return columns


def _quote_identifier(identifier: str) -> str:
    if not identifier:
        raise ValueError("Snowflake identifiers must not be empty")
    return '"' + identifier.replace('"', '""') + '"'


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_csvs(data_dir: Path, cursor: Any, database: str, schema: str) -> list[str]:
    """Upload CSVs and COPY them into header-derived VARCHAR tables."""
    data_dir = data_dir.expanduser().resolve()
    if not data_dir.is_dir():
        raise FileNotFoundError(f"CSV input directory does not exist: {data_dir}")

    files = sorted(data_dir.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    tables: dict[str, Path] = {}
    for csv_path in files:
        table = normalize_identifier(csv_path.stem)
        if table in tables:
            raise ValueError(
                f"CSV filenames {tables[table].name!r} and {csv_path.name!r} "
                f"both map to Snowflake table {table}"
            )
        tables[table] = csv_path

    db = _quote_identifier(database)
    sch = _quote_identifier(schema)
    stage_name = _quote_identifier("RETAILFLOW_INGEST")
    qualified_schema = f"{db}.{sch}"
    qualified_stage = f"{qualified_schema}.{stage_name}"

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}")
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {qualified_stage}")

    loaded: list[str] = []
    for table, csv_path in tables.items():
        columns = csv_columns(csv_path)
        target = f"{qualified_schema}.{_quote_identifier(table)}"
        column_ddl = ", ".join(
            f"{_quote_identifier(column)} VARCHAR" for column in columns
        )
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {target} ({column_ddl})")
        cursor.execute(f"DESC TABLE {target}")
        existing_columns = [row[0] for row in cursor.fetchall()]
        if existing_columns != columns:
            raise ValueError(
                f"CSV header in {csv_path.name!r} does not match existing "
                f"Snowflake table {table}: expected {existing_columns}, found {columns}"
            )

        cursor.execute(
            f"PUT {_sql_literal(csv_path.as_uri())} {qualified_stage} "
            "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        )
        staged_filename = f"{csv_path.name}.gz"
        cursor.execute(
            f"COPY INTO {target} FROM {qualified_stage} "
            f"FILES=({_sql_literal(staged_filename)}) "
            """FILE_FORMAT=(TYPE=CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"') """
            "ON_ERROR='ABORT_STATEMENT'"
        )
        loaded.append(table)

    return loaded
