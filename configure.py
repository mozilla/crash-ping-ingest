#!/usr/bin/env python3
# Print ingester configuration to stdout.

from datetime import datetime, timedelta, timezone
import os
import requests

UserAgent = "crash-ping-ingest/1.0 configure"

session = requests.Session()
session.headers.update({'User-Agent': UserAgent})

def maybe_correct_version(now_date, chan, version_field, json_req):
    date_string = json_req[chan]
    if len(date_string) == 25:
        chan_date = datetime.fromisoformat(date_string)
    elif len(date_string) == 10:
        chan_date = datetime.strptime(date_string, "%Y-%m-%d").astimezone(timezone.utc)
    else:
        raise ValueError(f"Unexpected date string length: {len(date_string)}")
    version = int(json_req[version_field].split('.')[0])
    diff = now_date - chan_date
    if diff < timedelta(days=2):
        version -= 1
        print(f"[{chan}] Detected {diff} time difference, fallback to {version}")
    return version

def get_versions():
    rv = {}
    now_date = datetime.now(timezone.utc)
    for (chan, chan_date) in [("nightly", "nightly_start"), ("beta", "beta_1")]:
        base_url = f"https://whattrainisitnow.com/api/release/schedule/?version={chan}"
        req = session.get(base_url)
        if not req.ok:
            raise IndexError
        rv[chan] = maybe_correct_version(now_date, chan_date, "version", req.json())

    req = session.get("https://product-details.mozilla.org/1.0/firefox_versions.json")
    if not req.ok:
        raise IndexError
    rv["release"] = maybe_correct_version(now_date, "LAST_RELEASE_DATE", "LATEST_FIREFOX_VERSION", req.json())

    return rv

if __name__ == "__main__":
    api_key = os.getenv("REDASH_API_KEY")
    if not api_key:
        print("Please set REDASH_API_KEY env")
        exit(1)
    print(f"redash.api_key = '{api_key}'")
    versions = get_versions()
    print(f"redash.matrix.channel.nightly.version = '{versions['nightly']}'")
    print(f"redash.matrix.channel.beta.version = '{versions['beta']}'")
    print(f"redash.matrix.channel.release.version = '{versions['release']}'")
