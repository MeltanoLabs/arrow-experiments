"""Schema discovery via INFORMATION_SCHEMA."""

from __future__ import annotations

from typing import Any


# Map MySQL DATA_TYPE to JSON Schema type + format
_TYPE_MAP: dict[str, dict] = {
    "tinyint": {"type": ["null", "integer"]},
    "smallint": {"type": ["null", "integer"]},
    "mediumint": {"type": ["null", "integer"]},
    "int": {"type": ["null", "integer"]},
    "bigint": {"type": ["null", "integer"]},
    "float": {"type": ["null", "number"]},
    "double": {"type": ["null", "number"]},
    "decimal": {"type": ["null", "number"]},
    "numeric": {"type": ["null", "number"]},
    "bit": {"type": ["null", "integer"]},
    "bool": {"type": ["null", "boolean"]},
    "boolean": {"type": ["null", "boolean"]},
    "date": {"type": ["null", "string"], "format": "date"},
    "datetime": {"type": ["null", "string"], "format": "date-time"},
    "timestamp": {"type": ["null", "string"], "format": "date-time"},
    "time": {"type": ["null", "string"]},
    "year": {"type": ["null", "integer"]},
    "json": {"type": ["null", "string"]},
}


def _json_schema_for(data_type: str, column_type: str) -> dict:
    """Return JSON Schema entry for a MySQL column."""
    dt = data_type.lower()
    ct = column_type.lower()
    # tinyint(1) is conventionally used as boolean
    if dt == "tinyint" and ct == "tinyint(1)":
        return {"type": ["null", "boolean"]}
    return _TYPE_MAP.get(dt, {"type": ["null", "string"]})


def discover_catalog(conn, config: dict[str, Any]) -> dict:
    """Query INFORMATION_SCHEMA and return a Singer catalog dict."""
    filter_dbs: list[str] = [db.strip() for db in config.get("filter_dbs", "").split(",") if db.strip()]
    excluded = ("information_schema", "performance_schema", "mysql", "sys")

    query = (
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, "
        "COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA NOT IN ({excluded}) "
        "{filter_clause}"
        "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
    )
    excluded_placeholders = ", ".join(["?"] * len(excluded))
    params: list = list(excluded)

    if filter_dbs:
        filter_placeholders = ", ".join(["?"] * len(filter_dbs))
        filter_clause = f"AND TABLE_SCHEMA IN ({filter_placeholders}) "
        params.extend(filter_dbs)
    else:
        filter_clause = ""

    sql = query.format(
        excluded=excluded_placeholders,
        filter_clause=filter_clause,
    )

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    # Group rows by (schema, table)
    tables: dict[tuple[str, str], dict] = {}
    for (
        table_schema,
        table_name,
        col_name,
        data_type,
        col_type,
        _is_nullable,
        col_key,
    ) in rows:
        key = (table_schema, table_name)
        if key not in tables:
            tables[key] = {"properties": {}, "primary_keys": []}
        tables[key]["properties"][col_name] = _json_schema_for(data_type, col_type)
        if col_key == "PRI":
            tables[key]["primary_keys"].append(col_name)

    streams = []
    for (db, table), info in tables.items():
        stream_name = f"{db}-{table}"
        streams.append(
            {
                "stream": stream_name,
                "tap_stream_id": stream_name,
                "schema": {
                    "type": "object",
                    "properties": info["properties"],
                    "additionalProperties": False,
                },
                "key_properties": info["primary_keys"],
                "metadata": [
                    {
                        "breadcrumb": [],
                        "metadata": {
                            "selected": True,
                            "replication-method": "FULL_TABLE",
                            "database-name": db,
                            "table-name": table,
                            "row-count": 0,
                        },
                    }
                ],
            }
        )

    return {"streams": streams}
