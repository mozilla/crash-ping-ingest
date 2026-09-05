#!/usr/bin/env python3
# Download crash pings from live tables, symbolicate, and upload results.

from concurrent.futures import InterpreterPoolExecutor, as_completed
from concurrent.interpreters import create_queue, Queue
from google.cloud import bigquery
import json
import logging
import os
import subprocess
import typing
import uuid

PROJECT = 'moz-fx-data-shared-prod'
STAGE_PROJECT = 'moz-fx-data-shar-nonprod-efed'
DATASET = 'crash_ping_ingest_external'
INGESTER = 'ingester'

OUTPUT_TABLE_SCHEMA = [
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
            bigquery.SchemaField('offset', 'STRING',
                description = "The absolute offset of the frame.",
            ),
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


def process_pings(rows: bigquery.table.RowIterator) -> str | list[typing.Dict[str, typing.Any]]:
    input = "\n".join(json.dumps(dict(row)) for row in rows)
    symbolicated = subprocess.run([INGESTER, "--no-progress", "keep_going=true"], text=True, input=input, capture_output=True)

    if symbolicated.returncode != 0:
        return f"ingester returned {symbolicated.returncode}: {symbolicated.stderr}"

    output = symbolicated.stdout
    return [json.loads(s) for s in output.strip().split("\n")]


class BQ:
    _client: bigquery.Client
    _download_query: str
    _task_id: str

    def __init__(self, task_id: str, project: str):
        self._client = bigquery.Client(project)
        with open("download-live.sql", 'r') as f:
            self._download_query = f.read()
        self._task_id = task_id
        self.create_tables()

    def create_tables(self):
        tables = list(self._client.list_tables(DATASET))
        if not any(x.table_id == "live_ingest_log" for x in tables):
            table_ref = self._client.dataset(DATASET).table("live_ingest_log")
            table = bigquery.Table(table_ref, schema = [
                bigquery.SchemaField('time', 'TIMESTAMP',
                    description = "The time processing began.",
                    mode = 'REQUIRED',
                ),
                bigquery.SchemaField('task', 'STRING',
                    description = "The task processing the ping.",
                    mode = 'REQUIRED',
                ),
                bigquery.SchemaField('document_id', 'STRING',
                    description = "The document_id of the ping.",
                    mode = 'REQUIRED',
                ),
            ])
            self._client.create_table(table)
        if not any(x.table_id == "live_ingest_output" for x in tables):
            table_ref = self._client.dataset(DATASET).table("live_ingest_output")
            table = bigquery.Table(table_ref, schema = OUTPUT_TABLE_SCHEMA)
            table._properties["tableConstraints"] = {}
            table._properties["tableConstraints"]["primaryKey"] = {"columns": ["document_id"]}
            # We can't add a foreign key because the ids may pertain to ids in different glean tables.
            table.time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = "submission_timestamp",
                # 775 days, matching the crash tables
                expiration_ms = 1000 * 60 * 60 * 24 * 775,
                require_partition_filter = True,
            )

            self._client.create_table(table)

    def download(self) -> tuple[int, bigquery.table.RowIterator]:
        job_config = bigquery.QueryJobConfig(
            query_parameters = [
                bigquery.ScalarQueryParameter("task_id", bigquery.SqlParameterScalarTypes.STRING, self._task_id),
            ],
        )
        query_job = self._client.query(self._download_query, job_config = job_config)
        # Wait for job to complete (to ensure all child jobs are present)
        query_job.result()
        child_jobs = self._client.list_jobs(parent_job = query_job)
        select_data_job, count_job, *_ = (job for job in child_jobs if job.statement_type == "SELECT")
        return next(count_job.result(max_results=1))[0], select_data_job.result()

    def upload(self, data):
        self._client.load_table_from_json(
            data,
            self._client.dataset(DATASET).table("live_ingest_output")
        ).result()

    def clear_log(self):
        self._client.query_and_wait(
            f"DELETE FROM {DATASET}.live_ingest_log where task = @task_id;",
            job_config = bigquery.QueryJobConfig(
                query_parameters = [
                    bigquery.ScalarQueryParameter("task_id", bigquery.SqlParameterScalarTypes.STRING, self._task_id),
                ],
            ),
        )


if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--loglevel', default='info', help="set the log level", choices=['critical','error','warning','info','debug'])
    parser.add_argument('--production', action='store_true', help="use production tables")

    args = parser.parse_args()

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(level=args.loglevel.upper(), format="%(asctime)s %(levelname)s (%(name)s): %(message)s", datefmt="%Y-%m-%dT%H:%M:%SZ")
    task_id = os.environ.get("TASK_ID") or str(uuid.uuid4())
    logger = logging.getLogger("main")

    total_rows = 0
    success_rows = 0
    bq = BQ(task_id, PROJECT if args.production else STAGE_PROJECT)
    while True:
        logger.info("querying ping data")
        avail_count, pings = bq.download()
        try:
            # Stop when we are getting fewer results to let a subsequent run do
            # the work.
            if avail_count < 100:
                logger.info(f"not enough rows ({avail_count} available): exiting")
                break

            logger.info(f"received {pings.total_rows} pings")

            total_rows += pings.total_rows
            result_rows = process_pings(pings)
            success_rows += len(result_rows)

            logger.info(f"read {total_rows} ({total_rows-success_rows} failed)")

            bq.upload(result_rows)
        finally:
            bq.clear_log()
