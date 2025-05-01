#!/usr/bin/env python3

# Upload processed crash ping data to bigquery.

from datetime import date
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
from pathlib import Path
import json
import os
import random
import sys
import time
import typing

PROJECT = 'moz-fx-data-shared-prod'
STAGE_PROJECT = 'moz-fx-data-shar-nonprod-efed'

DATASET = 'crash_ping_ingest_external'

# NOTE: This script will only deal with column changes. Changing internal
# information like column types won't be caught by `update_table_schema`.

TABLE_NAME = 'ingest_output'
TABLE_SCHEMA = [
    bigquery.SchemaField('document_id', 'STRING',
        description = "Identifier matching the document_id of a submitted ping.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('submission_timestamp', 'TIMESTAMP',
        description = "The submission timestamp matching that of the submitted ping (used for partitioning).",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('config_id', 'INTEGER',
        description = "The id of the configuration partition that this ping was in.",
    ),
    bigquery.SchemaField('crash_type', 'STRING',
        description = "The crash type, extracted from the ping stack trace data for convenience."
    ),
    bigquery.SchemaField('signature', 'STRING',
        description = "The crash signature generated from the symbolicated stack frames."
    ),
    bigquery.SchemaField('stack', 'RECORD',
        description = "Symbolicated stack frames for the crash ping.",
        mode = 'REPEATED',
        fields = [
            bigquery.SchemaField('function', 'STRING',
                description = "The function symbol corresponding to the stack frame.",
            ),
            bigquery.SchemaField('function_offset', 'STRING',
                description = "The offset into the function, as a hex string.",
            ),
            bigquery.SchemaField('file', 'STRING',
                description = "The source file corresponding to the stack frame.",
            ),
            bigquery.SchemaField('line', 'INTEGER',
                description = "The source line corresponding to the stack frame.",
            ),
            bigquery.SchemaField('module', 'STRING',
                description = "The module corresponding to the stack frame.",
            ),
            bigquery.SchemaField('module_offset', 'STRING',
                description = "The offset into the module, as a hex string.",
            ),
            bigquery.SchemaField('omitted', 'INTEGER',
                description = "Whether frames were omitted. If this field is present, no other fields will be present.",
            ),
            bigquery.SchemaField('error', 'STRING',
                description = "An error message when generating the stack frame. If this field is present, no other fields will be present.",
            ),
        ],
    ),
]

CONFIG_TABLE_NAME = 'ingest_config'
CONFIG_TABLE_SCHEMA = [
    bigquery.SchemaField('id', 'INTEGER',
        description = "Unique integer (relative to 'date').",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('date', 'DATE',
        description = "The date for which the configuration applies.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('version', 'INTEGER',
        description = "The configured firefox version.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('target_sample_count', 'INTEGER',
        description = "The configured target sample count. Note that this may not match the actual sample count due to random sampling.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('os', 'STRING',
        description = "The configured os.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('channel', 'STRING',
        description = "The configured channel.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('process_type', 'STRING',
        description = "The configured process type.",
        mode = 'REQUIRED',
    ),
    bigquery.SchemaField('utility_actor', 'STRING',
        description = "The configured utility actor type. Only applicable when 'process_type' is 'utility'.",
    ),
    bigquery.SchemaField('count', 'INTEGER',
        description = "The count of pings matching the configuration.",
        mode = 'REQUIRED',
    ),
]

T = typing.TypeVar('T', bound=typing.Any)

class SessionConfigurator:
    def __init__(self, session_id: str):
        self._session_id = session_id

    def config(self, cfg: T) -> T:
        cfg.create_session = False
        props = cfg.connection_properties
        props.append(bigquery.query.ConnectionProperty(key="session_id", value=self._session_id))
        cfg.connection_properties = props
        return cfg

# Adapted from https://dev.to/stack-labs/bigquery-transactions-over-multiple-queries-with-sessions-2ll5
class BigquerySession:
    """ContextManager wrapping a bigquery session."""

    def __init__(self, bqclient: bigquery.Client):
        """Construct instance."""
        self._bigquery_client = bqclient
        self._session_id: typing.Optional[str] = None

    def __enter__(self) -> SessionConfigurator:
        """Initiate a Bigquery session and return the session_id."""
        job = self._bigquery_client.query(
            "SELECT 1;",  # a query that won't fail
            job_config=bigquery.QueryJobConfig(create_session=True),
        )
        assert job.session_info.session_id is not None
        self._session_cfg = SessionConfigurator(job.session_info.session_id)
        job.result()  # wait job completion
        return self._session_cfg

    def __exit__(self, exc_type, exc_value, traceback):
        """Abort the opened session."""
        if self._session_cfg:
            # abort the session in any case to have a clean state at the end
            # (sometimes in case of script failure, the table is locked in
            # the session)
            job = self._bigquery_client.query(
                "CALL BQ.ABORT_SESSION();",
                job_config=self._session_cfg.config(bigquery.QueryJobConfig())
            )
            job.result()

def create_table(client: bigquery.Client):
    table_ref = client.dataset(DATASET).table(TABLE_NAME)
    table = bigquery.Table(table_ref, schema = TABLE_SCHEMA)
    table._properties["tableConstraints"] = {}
    table._properties["tableConstraints"]["primaryKey"] = {"columns": ["document_id"]}
    # We can't add a foreign key because the ids may pertain to firefox_desktop or fenix tables.
    table.time_partitioning = bigquery.TimePartitioning(
        type_ = bigquery.TimePartitioningType.DAY,
        field = "submission_timestamp",
        # 775 days, matching the crash table
        expiration_ms = 1000 * 60 * 60 * 24 * 775,
        require_partition_filter = True,
    )

    client.create_table(table)

def schema_field_names(schema: list[bigquery.SchemaField]) -> set[str]:
    return set(f.name for f in schema)

def update_table_schema(client: bigquery.Client, table_name: str, schema: list[bigquery.SchemaField]):
    table_ref = client.dataset(DATASET).table(table_name)
    table = client.get_table(table_ref)

    if schema_field_names(table.schema) != schema_field_names(schema):
        table.schema = schema
        client.update_table(table, ["schema"])

def create_config_table(client: bigquery.Client):
    table_ref = client.dataset(DATASET).table(CONFIG_TABLE_NAME)
    table = bigquery.Table(table_ref, schema = CONFIG_TABLE_SCHEMA)
    client.create_table(table)

def project_id(prod: bool = False) -> str:
    return PROJECT if prod else STAGE_PROJECT

def make_client(prod: bool = False) -> bigquery.Client:
    return bigquery.Client(project = project_id(prod))

def try_upload(client: bigquery.Client, 
               data_date: date,
               ping_ndjson: tuple[typing.IO[bytes], typing.Optional[int]],
               config_ndjson: tuple[typing.IO[bytes], int]) -> bool:
    try:
        with BigquerySession(client) as session:
            client.query_and_wait("BEGIN TRANSACTION;", job_config=session.config(bigquery.QueryJobConfig()))

            # Clear out rows already associated with the date. We want all rows for
            # a particular date to be the result of a single upload.
            client.query_and_wait((
                    f"DELETE FROM {DATASET}.{CONFIG_TABLE_NAME} where date = @date;"
                    f"DELETE FROM {DATASET}.{TABLE_NAME} where DATE(submission_timestamp) = @date;"
                ),
                job_config = session.config(bigquery.QueryJobConfig(
                    query_parameters = [
                        bigquery.ScalarQueryParameter("date", bigquery.SqlParameterScalarTypes.DATE, data_date),
                    ],
                )),
            )

            # Upload new data
            client.load_table_from_file(
                ping_ndjson[0],
                client.dataset(DATASET).table(TABLE_NAME),
                size = ping_ndjson[1],
                rewind = True,
                job_config = session.config(bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
            ).result()

            client.load_table_from_file(
                config_ndjson[0],
                client.dataset(DATASET).table(CONFIG_TABLE_NAME),
                size = config_ndjson[1],
                rewind = True,
                job_config = session.config(bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON))
            ).result()

            client.query("COMMIT TRANSACTION;", job_config=session.config(bigquery.QueryJobConfig())).result()

            return True
    except BadRequest as e:
        if "aborted due to concurrent update" in str(e):
            return False
        raise


def upload(prod: bool,
           data_date: date,
           ping_ndjson: tuple[typing.IO[bytes], typing.Optional[int]],
           config_ndjson: tuple[typing.IO[bytes], int]):
    client = make_client(prod)

    tables = list(client.list_tables(DATASET))
    if not any(x.table_id == TABLE_NAME for x in tables):
        create_table(client)
    if not any(x.table_id == CONFIG_TABLE_NAME for x in tables):
        create_config_table(client)

    update_table_schema(client, TABLE_NAME, TABLE_SCHEMA)
    update_table_schema(client, CONFIG_TABLE_NAME, CONFIG_TABLE_SCHEMA)

    # We may fail due to concurrent updates by other tasks (if a batch of tasks is started); just keep retrying for a while.
    retries = 30
    while not try_upload(client, data_date, ping_ndjson, config_ndjson):
       time.sleep(random.randint(1, 5) * 30) 
       retries -= 1
       if retries == 0:
           raise RuntimeError("aborted due to concurrent update; retries exhausted")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--production', action='store_true', help="upload to production tables")
    parser.add_argument('config', help="the config table jsonl file to read")
    parser.add_argument('filename', help="the jsonl file to read; use '-' for stdin")

    args = parser.parse_args()

    size = None
    data = None

    if args.filename == '-':
        data = sys.stdin.buffer
    else:
        path = Path(args.filename)
        size = path.stat().st_size
        data = path.open('rb')

    config_path = Path(args.config)
    config_path_size = config_path.stat().st_size
    config = config_path.open('rb')

    # Peek at the config to get the date
    data_date = date.fromisoformat(json.loads(config.readline())["date"])

    upload(args.production, data_date, (data, size), (config, config_path_size))
