#!/usr/bin/env python3
# Fetch data for the given date.
#
# This also derives the active release version for the date to use in the query.

from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
from google.cloud import bigquery
import requests

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

def download(query: str, for_date: date, release_version: int) -> bigquery.table.RowIterator:
    client = bigquery.Client(PROJECT)
    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("date", bigquery.SqlParameterScalarTypes.DATE, for_date),
            bigquery.ScalarQueryParameter("release_version", bigquery.SqlParameterScalarTypes.INT64, release_version),
        ],
    )
    return client.query_and_wait(query, job_config = job_config)


if __name__ == "__main__":
    import argparse
    import json
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date', help="the date for which to fetch data. Defaults to yesterday (UTC)")
    parser.add_argument('-q', '--query', metavar="FILE", help="""the sql query file to use to fetch the data.
                        It will be passed `date: DATE` and `release_version: INT64` parameters.
                        Defaults to `download.sql`""", default="download.sql")
    parser.add_argument('filename', help="the jsonl file to write; use '-' for stdout")

    args = parser.parse_args()

    for_date = date.fromisoformat(args.date) if args.date else datetime.now(timezone.utc).date() - timedelta(days = 1)
    release_version = get_release_version(for_date)

    with open(args.query, 'r') as f:
        query = f.read()

    if args.filename == '-':
        output = sys.stdout
    else:
        output = open(args.filename, 'w')

    for row in download(query, for_date, release_version):
        json.dump(dict(row), output, separators=(',',':'))
        output.write("\n")
