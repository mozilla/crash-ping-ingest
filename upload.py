#!/usr/bin/env python3

# Upload processed crash ping data to bigquery.

from google.cloud import bigquery
from pathlib import Path
import os
import sys
import typing

PROJECT = 'moz-fx-data-shared-prod'
STAGE_PROJECT = 'moz-fx-data-shar-nonprod-efed'

DATASET = 'crash_ping_ingest_external'

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

def create_table(client: bigquery.Client):
    table_ref = client.dataset(DATASET).table(TABLE_NAME)
    table = bigquery.Table(table_ref, schema = TABLE_SCHEMA)
    table._properties["tableConstraints"] = {}
    table._properties["tableConstraints"]["primaryKey"] = {"columns": ["document_id"]}
    # We can't add a foreign key because the ids may pertain to firefox_desktop or android tables.
    table.time_partitioning = bigquery.TimePartitioning(
        type_ = bigquery.TimePartitioningType.DAY,
        field = "submission_timestamp",
        # 775 days, matching the crash table
        expiration_ms = 1000 * 60 * 60 * 24 * 775,
        require_partition_filter = True,
    )

    client.create_table(table)

def project_id(prod: bool = False) -> str:
    return PROJECT if prod else STAGE_PROJECT

def make_client(prod: bool = False) -> bigquery.Client:
    return bigquery.Client(project = project_id(prod))


def upload(prod: bool, ping_ndjson_data: typing.IO[bytes], size: typing.Optional[int] = None):
    client = make_client(prod)

    tables = list(client.list_tables(DATASET))
    if not any(x.table_id == TABLE_NAME for x in tables):
        create_table(client)

    load_job = client.load_table_from_file(
        ping_ndjson_data,
        client.dataset(DATASET).table(TABLE_NAME),
        size = size,
        job_config = bigquery.LoadJobConfig(source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    )

    load_job.result()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--production', action='store_true', help="upload to production tables")
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

    upload(args.production, data, size)
