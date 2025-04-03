# crash-ping-ingest

The program in [ingester](./ingester) ingests crash pings as [jsonl][], symbolicates them (locally),
and outputs the resulting crash ping signatures and stacks as [jsonl][]. It does so as quickly as
possible with the resources provisioned (you may limit threads, cache size, download count, etc).

[download.py](./download.py) figures out the channel versions for a given date and downloads data
from bigquery.

[upload.py](./upload.py) uploads the output of the ingester (whether through stdin or a file) to
bigquery tables.

Both `download.py` and `upload.py` require you to set up credentials through
`GOOGLE_APPLICATION_CREDENTIALS` or other means.

## Performance

While the crash ping ingestion attempts to use the allocated resources as efficiently as possible, I
suspect that the signature generation may be a bottleneck right now (without having looked too
closely into it). We have to launch a python program to generate each signature; it may end up a lot
more efficient to launch a python server and send signature requests to it. Alternatively, linking
and launching a python runtime in-process would potentially avoid doing any IPC and serialization.

## Taskcluster

The taskcluster tasks are configured such that, when started as a cron job, ping data is processed
for any days in the past 7 days which don't yet have processed data. The goal is to have ingested
data for as much of history as possible, however we allow a 7-day buffer in case the CI is broken
(from internal or external factors), with the assumption that it will be repaired within that
window. If specific data is missing, the task can be manually run: there is an action on the task
group to manually run for a given date.

[jsonl]: https://jsonlines.org/
