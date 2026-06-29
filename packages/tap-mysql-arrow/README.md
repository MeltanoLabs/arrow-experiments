# tap-mysql-arrow

Singer tap for MySQL/MariaDB that emits Arrow [BATCH](https://hub.meltano.com/singer/spec/#batch-messages) messages via [ADBC](https://docs.adbc-drivers.org/drivers/mysql/).

## Requirements

Install the ADBC MySQL driver:

```bash
dbc install mysql
```

Then install this package:

```bash
pip install tap-mysql-arrow
```

## Configuration

| Setting | Required | Default | Description |
|---|---|---|---|
| `host` | Yes | | MySQL/MariaDB host |
| `port` | Yes | `3306` | MySQL/MariaDB port |
| `user` | Yes | | Database user |
| `password` | Yes | | Database password |
| `database` | No | | Default database |
| `filter_dbs` | No | | Comma-separated list of databases to sync |
| `ssl_ca` | No | | Path to PEM CA certificate |
| `ssl_cert` | No | | Path to PEM client certificate |
| `ssl_key` | No | | Path to PEM client key |
| `ssh_host` | No | | SSH tunnel host |
| `ssh_port` | No | `22` | SSH tunnel port |
| `ssh_username` | No | | SSH username |
| `ssh_private_key` | No | | Path to SSH private key file |
| `ssh_private_key_password` | No | | SSH private key passphrase |
| `ssh_password` | No | | SSH password (alternative to key) |
| `batch_size` | No | `500000` | Rows per Arrow IPC file |
| `batch_root_dir` | No | System temp | Directory for Arrow IPC files |
| `session_sqls` | No | See below | SQL statements run on connect |

Default `session_sqls`:
```json
[
  "SET @@session.time_zone='+0:00'",
  "SET @@session.wait_timeout=28800",
  "SET @@session.net_read_timeout=3600",
  "SET @@session.innodb_lock_wait_timeout=3600"
]
```

## Usage

```bash
# Discovery
tap-mysql-arrow --config config.json --discover > catalog.json

# Sync
tap-mysql-arrow --config config.json --catalog catalog.json
```

Pipe into a mapper or target:

```bash
tap-mysql-arrow --config tap-config.json --catalog catalog.json \
  | mapper-fivetran-arrow \
  | target-snowflake-arrow --config target-config.json
```

## Output format

Emits Singer `SCHEMA` then one or more `BATCH` messages per stream, each referencing Arrow IPC files:

```json
{"type": "BATCH", "stream": "mydb-mytable", "encoding": {"format": "arrow"}, "manifest": ["file:///tmp/mydb-mytable_0.arrow"]}
```

Record counts are emitted to stderr as Singer `METRIC` messages every 60 seconds per stream.
