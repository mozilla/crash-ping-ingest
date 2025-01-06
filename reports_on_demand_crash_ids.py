#!/usr/bin/env python3
# Ingest crash information from json files on the command line, select crash
# ids to use, and output to stdout.

from collections import Counter
import hashlib
import requests
from urllib.parse import urlencode

UserAgent = "crash-ping-ingest/1.0 reports_on_demand_crash_ids"

# For each of the top signatures by client crash volume:
IdsForMostCommon = 10
# we want to produce a list of crash ids if we don't have many reports:
MinimumReports = 10
# partitioning configurations on:
ConfigurationParameters = ["os", "osversion", "arch", "version"]
# where we limit crash ids per configuration:
IdsPerConfiguration = 100
# and we limit crash ids per client:
IdsPerClient = 1


session = requests.Session()
session.headers.update({'User-Agent': UserAgent})


def signature_hash(signature):
  return hashlib.md5(signature.encode('utf-8')).hexdigest()


def pings_by_signature(processed):
  by_signature = {}
  for ping in processed:
    if 'signature' not in ping:
      continue
    hash = signature_hash(ping['signature'])
    if hash not in by_signature:
      by_signature[hash] = {
          'signature': ping['signature'],
          'pings': []
      }
    by_signature[hash]['pings'].append(ping)

  return by_signature


def client_count(pings):
  return len(frozenset([ping['clientid'] for ping in pings]))


def need_reports(signature):
  req = session.get("https://crash-stats.mozilla.org/api/SuperSearch/", params={'signature': f"={signature}", '_results_number': 0})
  if not req.ok:
    return True
  return req.json()['total'] < MinimumReports


def top_crasher_ids(processed):
  by_signature = pings_by_signature(processed)

  signature_client_counts = Counter()
  for hash, data in by_signature.items():
    signature_client_counts[hash] = client_count(data['pings'])

  results = {}

  for hash, _ in signature_client_counts.most_common():
    data = by_signature[hash]
    signature = data['signature']

    # Before collecting ids, check whether we have reports on Socorro. If we
    # do, we can skip the signature (we want to target signatures which are
    # under-reported).
    if not need_reports(signature):
      continue

    clients = Counter()
    configurations = Counter()
    ids = []

    for ping in data['pings']:
      client = ping['clientid']
      configuration = tuple([ping[k] for k in ConfigurationParameters])

      if (configurations[configuration] >= IdsPerConfiguration
          or clients[client] >= IdsPerClient
          or 'minidump_sha256_hash' not in ping
          or ping['minidump_sha256_hash'] is None):
        continue

      configurations[configuration] += 1
      clients[client] += 1

      ids.append(ping['minidump_sha256_hash'])

    # Some signatures are from circumstances where we will never get minidumps;
    # skip these since the reports will not have much more information.
    if len(ids) == 0:
      continue

    results[hash] = { "hashes": ids, "description": signature } 
    
    if len(results) >= IdsForMostCommon:
      break

  return results


if __name__ == "__main__":
  import gzip
  import json
  import sys

  processed = []
  for file in sys.argv[1:]:
    processed += json.load(gzip.open(file, 'r') if file.endswith(".gz") else open(file))

  json.dump(top_crasher_ids(processed), sys.stdout)
