#!/usr/bin/env python3
# Print ingester configuration with the appropriate version for the given date
# (defaulting to yesterday) to stdout.
#
# This also uses the REDASH_API_KEY env variable to securely pass the secret to
# the ingester.

from bisect import bisect_right
from datetime import date, datetime, timedelta, timezone
import requests

RELEASE_ROLLOUT_PERIOD = timedelta(days=2)

USER_AGENT = "crash-ping-ingest/1.0 date_version_config"

session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})

def get_release_version(for_date):
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

if __name__ == "__main__":
    import os
    import sys
    api_key = os.getenv("REDASH_API_KEY")
    if not api_key:
        print("Please set REDASH_API_KEY env variable")
        exit(1)
    print(f"redash.api_key = '{api_key}'")

    for_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) == 2 else datetime.now(timezone.utc).date() - timedelta(days = 1)
    version = get_release_version(for_date)

    print(f"redash.parameters.date = '{for_date}'")
    print(f"redash.parameters.release_version = '{version}'")
