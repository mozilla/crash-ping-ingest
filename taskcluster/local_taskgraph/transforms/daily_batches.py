import datetime
from urllib.request import Request, urlopen
from taskgraph.transforms.base import TransformSequence
from taskgraph.util.schema import Schema
from voluptuous import Required, ALLOW_EXTRA

CI_INDEX_API = "https://firefox-ci-tc.services.mozilla.com/api/index/v1"

CRON_DAILY_BATCHES_SCHEMA = Schema({
    # If specified, `days` copies of the task will be run.
    "cron-daily-batches": {
        # The number of days preceding the cron run date for which to generate tasks.
        Required("days"): int,
        # The index path to use, both to index the results and to avoid running
        # a task when one has already run. The `{date}` string will be
        # interpolated with the batch date.
        "index": str,
        # An environment variable name which (if specified) will be set with the batch date.
        "env": str,
    }
}, extra=ALLOW_EXTRA)


transforms = TransformSequence()
transforms.add_validate(CRON_DAILY_BATCHES_SCHEMA)


def index_task_exists(index):
    url = f"{CI_INDEX_API}/task/{index}"
    request = Request(url, headers={"User-Agent": "crash-ping-ingest/1.0 daily_batches"}, method="HEAD")
    return urlopen(request).status == 200


@transforms.add
def create_daily_tasks(config, tasks):
    """Duplicates a task for each of the days that is missing and sets the
    appropriate routes.

    Only applies to `cron` jobs.
    """
    if config.params["tasks_for"] != "cron":
        yield from tasks
        return

    today = datetime.datetime.now(datetime.UTC).date()

    for task in tasks:
        daily_batches = task.pop('cron-daily-batches', None)
        if daily_batches is None:
            yield task
            continue

        days = daily_batches["days"]
        index = daily_batches.get("index")
        env = daily_batches.get("env")

        # We only create tasks for complete days: `days` will immediately
        # *precede* `today`.
        for preceding in range(-days, 0):
            daily_task = task.copy()
            date = today + datetime.timedelta(days = preceding)

            if index is not None:
                index = index.format(date=date)
                if index_task_exists(index):
                    continue
                daily_task.setdefault("routes", []).append(f"index.{index}")

            if env is not None:
                daily_task["worker"].setdefault("env", {})[env] = str(date)

            yield daily_task

