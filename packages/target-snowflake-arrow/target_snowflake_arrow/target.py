"""target-snowflake-arrow: Singer target loading Arrow BATCH messages via ADBC."""

from __future__ import annotations

from singer_sdk import Target

from target_snowflake_arrow.sinks import SnowflakeArrowSink


class TargetSnowflakeArrow(Target):
    name = "target-snowflake-arrow"
    default_sink_class = SnowflakeArrowSink

    config_jsonschema = {
        "type": "object",
        "required": ["user", "account", "database"],
        "properties": {
            # Identity
            "user": {"type": "string", "description": "Snowflake login name"},
            "account": {
                "type": "string",
                "description": "Snowflake account identifier (e.g. xy12345.us-east-1)",
            },
            "database": {"type": "string", "description": "Target database"},
            "schema": {
                "type": "string",
                "description": "Target schema (default: PUBLIC)",
                "default": "PUBLIC",
            },
            "warehouse": {"type": "string", "description": "Snowflake warehouse"},
            "role": {"type": "string", "description": "Snowflake role"},
            # Authentication — one of password / private_key / private_key_path /
            # use_browser_authentication is required at runtime
            "password": {"type": "string", "description": "Password authentication"},
            "private_key": {
                "type": "string",
                "description": "PEM-encoded private key (inline)",
            },
            "private_key_path": {
                "type": "string",
                "description": "Path to PEM private key file",
            },
            "private_key_passphrase": {
                "type": "string",
                "description": "Passphrase for encrypted private key",
            },
            "use_browser_authentication": {
                "type": "boolean",
                "default": False,
                "description": "Authenticate via external browser (SSO)",
            },
        },
        "additionalProperties": False,
    }


if __name__ == "__main__":
    TargetSnowflakeArrow.cli()
