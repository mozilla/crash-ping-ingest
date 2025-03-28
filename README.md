# crash-ping-ingest

The program in [ingester](./ingester) ingests crash pings as [jsonl][], symbolicates them (locally),
and outputs the resulting crash ping signatures and stacks as [jsonl][]. It does so as quickly as
possible with the resources provisioned (you may limit threads, cache size, download count, etc).

[date_version_config.py](./date_version_config.py) figures out the channel versions for a given date
and reads the `REDASH_API_KEY` environment variable to output configuration for the ingester (which
can be piped in stdin).

[upload.py](./upload.py) uploads the output of the ingester (whether through stdin or a file) to
bigquery tables (you must set up credentials through `GOOGLE_APPLICATION_CREDENTIALS` or other
means).

## Taskcluster

The taskcluster tasks are configured such that, when started as a cron job, ping data is processed
for any days in the past 7 days which don't yet have processed data. The goal is to have ingested
data for as much of history as possible, however we allow a 7-day buffer in case the CI is broken
(from internal or external factors), with the assumption that it will be repaired within that
window. If specific data is missing, the task can be manually run: there is an action on the task
group to manually run for a given date.

[jsonl]: https://jsonlines.org/
