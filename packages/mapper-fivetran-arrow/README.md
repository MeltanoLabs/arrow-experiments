# mapper-fivetran-arrow

Singer mapper that applies Fivetran-style field transforms to Arrow [BATCH](https://hub.meltano.com/singer/spec/#batch-messages) messages.

## Installation

```bash
pip install mapper-fivetran-arrow
```

## What it does

For each Arrow BATCH file received:

- **Field name normalisation** — converts all column names to `snake_case` (handles `camelCase`, `PascalCase`, `UPPER_CASE`, and dot-separated names)
- **`_fivetran_id`** — added as the sole key property when the stream has no primary keys; value is the MD5 hex digest of the full row serialised as JSON
- **`_fivetran_synced`** — set from `_sdc_extracted_at` if present, otherwise the current UTC time
- **`_fivetran_deleted`** — `true` when `_sdc_deleted_at` is non-null, otherwise `false`

`SCHEMA` messages are transformed in parallel so downstream consumers see the updated field names and system columns.

## Configuration

| Setting | Required | Default | Description |
|---|---|---|---|
| `batch_root_dir` | No | System temp | Directory for transformed Arrow IPC output files |

## Usage

```bash
tap-mysql-arrow --config tap-config.json --catalog catalog.json \
  | mapper-fivetran-arrow \
  | target-snowflake-arrow --config target-config.json
```
