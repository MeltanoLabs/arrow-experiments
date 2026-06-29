# target-snowflake-arrow

Singer target that loads Arrow [BATCH](https://hub.meltano.com/singer/spec/#batch-messages) messages into Snowflake via [ADBC](https://docs.adbc-drivers.org/drivers/snowflake/), truncating the destination table on each sync.

## Requirements

Install the ADBC Snowflake driver:

```bash
dbc install snowflake
```

Then install this package:

```bash
pip install target-snowflake-arrow
```

## Configuration

| Setting | Required | Default | Description |
|---|---|---|---|
| `user` | Yes | | Snowflake login name |
| `account` | Yes | | Account identifier (e.g. `xy12345.us-east-1`) |
| `database` | Yes | | Target database |
| `schema` | No | `PUBLIC` | Target schema |
| `warehouse` | No | | Snowflake warehouse |
| `role` | No | | Snowflake role |
| `password` | No | | Password (username/password auth) |
| `private_key_path` | No | | Path to PEM/p8 private key file (JWT auth) |
| `private_key` | No | | Inline PEM private key (JWT auth) |
| `private_key_passphrase` | No | | Passphrase for encrypted private key |
| `use_browser_authentication` | No | `false` | Authenticate via external browser (SSO) |

One of `password`, `private_key_path`, `private_key`, or `use_browser_authentication` must be provided.

## Usage

```bash
target-snowflake-arrow --config config.json
```

Pipe from a tap or mapper:

```bash
tap-mysql-arrow --config tap-config.json --catalog catalog.json \
  | mapper-fivetran-arrow \
  | target-snowflake-arrow --config target-config.json
```

## Load behaviour

The destination table is **truncated on each sync**: the first Arrow batch for a stream uses `mode=replace` (drop and recreate), and subsequent batches for the same stream append. This is safe when the full dataset is always reloaded.
