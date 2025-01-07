# crash-ping-ingest

The program in [ingester](./ingester) ingests crash pings, symbolicates them (locally), and outputs the resulting
information and crash ping signatures. It does so as quickly as possible with the resources
provisioned (you may limit threads, cache size, download count, etc).

Anecdotally, on an i9 system with a 20GB symbolication cache, this takes 45 minutes to do the same
work that takes about 7 hours with the old crash ping ingestion scripts (which use the symbolication
server).

[date_version_config.py](./date_version_config.py) figures out the channel versions for a given date
and reads the `REDASH_API_KEY` environment variable to output configuration for the ingester (which
can be piped in stdin).

[reports_on_demand_crash_ids.py](./reports_on_demand_crash_ids.py) reads processed ping output from
the ingester as file arguments and will choose crash ids to be sent through Remote Settings to get
more reports for particular signatures. It checks Socorro to determine whether particular signatures
need reports or not.

## Taskcluster

The taskcluster tasks are configured such that, when started as a cron job, ping data is processed
for any days in the past 7 days which don't yet have processed data. The goal is to have ingested
data for as much of history as possible, however we allow a 7-day buffer in case the CI is broken
(from internal or external factors), with the assumption that it will be repaired within that
window. If specific data is missing, the task can always be manually run.
