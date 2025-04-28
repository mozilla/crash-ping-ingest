#!/usr/bin/env python3
# Fetch data for the given date.
#
# This also derives the active release version for the date to use in the query.

from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery
from pathlib import PurePath
import requests
import typing

RELEASE_ROLLOUT_PERIOD = timedelta(days=2)

USER_AGENT = "crash-ping-ingest/1.0 date_version_config"

PROJECT = 'moz-fx-data-shared-prod'

session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})

def get_release_version(for_date: date) -> int:
    req = session.get("https://product-details.mozilla.org/1.0/firefox_history_major_releases.json")
    if not req.ok:
        raise IndexError
    # Take advantage of the JSON response having keys always in ascending
    # version (and date) order by loading the pairs directly into a list.
    release_versions = req.json(object_pairs_hook = list)

    # If the release has just switched over, keep the old versions for a little
    # while. Nightly/Beta/Release dates are sometimes a day apart, but we can
    # ignore that as this is just an affordance for the release roll out.
    search_date = for_date - RELEASE_ROLLOUT_PERIOD

    i = bisect_right([date for _, date in release_versions], str(search_date))
    major = int(release_versions[i - 1][0].split('.')[0]) if i > 0 else 0
    return major

# Returns the config rows and the pings rows
def download(query: str, for_date: date, release_version: int) -> tuple[bigquery.table.RowIterator, bigquery.table.RowIterator]:
    client = bigquery.Client(PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("date", bigquery.SqlParameterScalarTypes.DATE, for_date),
            bigquery.ScalarQueryParameter("release_version", bigquery.SqlParameterScalarTypes.INT64, release_version),
        ],
    )
    query_job = client.query(query, job_config = job_config)
    # Wait for job to complete (to ensure all child jobs are present)
    query_job.result()
    child_jobs = client.list_jobs(parent_job = query_job)
    [pings_job, config_job] = [job for job in child_jobs if job.statement_type == "SELECT"]
    return config_job.result(), pings_job.result()

def write_rows(rows: bigquery.table.RowIterator, output: typing.IO[str]):
    for row in rows:
        json.dump(dict(row), output, separators=(',',':'))
        output.write("\n")

if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help="the date for which to fetch data. Defaults to yesterday (UTC)")
    parser.add_argument('-q', '--query', metavar="FILE", help="""the sql query file to use to fetch the data.
                        It will be passed `date: DATE` and `release_version: INT64` parameters.
                        The query should have two select statements: the first producing configuration and
                        the second producing pings.
                        Defaults to `download.sql`""", default="download.sql")
    parser.add_argument('-c', '--config', metavar="FILE", help="the file to which to write the config file")
    parser.add_argument('filename', help="""the jsonl file to write; use '-' for stdout.
                        If `--config` is omitted, the config table will be written to `<filename-stem>-config.jsonl`, or just `config.jsonl` if writing to stdout.
                        """)

    args = parser.parse_args()

    for_date = date.fromisoformat(args.date) if args.date else datetime.now(timezone.utc).date() - timedelta(days = 1)
    release_version = get_release_version(for_date)

    with open(args.query, 'r') as f:
        query = f.read()

    config_output_path = args.config

    if args.filename == '-':
        pings_output = sys.stdout
        if config_output_path is None:
            config_output_path = 'config.jsonl'
    else:
        pings_output = open(args.filename, 'w')
        if config_output_path is None:
            path = PurePath(args.filename)
            config_output_path = str(path.with_stem(path.stem + "-config"))

    config_output = open(config_output_path, 'w')

    config, pings = download(query, for_date, release_version)
    write_rows(config, config_output)
    write_rows(pings, pings_output)
