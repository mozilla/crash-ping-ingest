from copy import deepcopy
import datetime
from urllib.request import Request, urlopen
from taskgraph.transforms.base import TransformSequence
from taskgraph.util.schema import Schema
from voluptuous import Required, ALLOW_EXTRA

CI_INDEX_API = "https://firefox-ci-tc.services.mozilla.com/api/index/v1"

ADDITIONAL_SCHEMA = Schema({
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
    },
    # If specified, dependencies corresponding to `days` tasks created by
    # `cron-daily-batches` will be added to the task.
    "cron-daily-dependencies": [{
        # The number of days preceding the cron run date for which to create
        # dependencies.  This should not exceed the number of days specified in
        # the corresponding `cron-daily-batches`.
        Required("days"): int,
        # The task name.
        Required("task"): str,
        # Artifacts to fetch. They will end up in `fetches/cron-daily-dependencies/{task}-N`.
        "artifacts": [str]
    }]
}, extra=ALLOW_EXTRA)


transforms = TransformSequence()
transforms.add_validate(ADDITIONAL_SCHEMA)


# Currently unused, however we'll keep it around in case we no longer want to
# use the index-search optimization.
def index_task_exists(index):
    url = f"{CI_INDEX_API}/task/{index}"
    request = Request(url, headers={"User-Agent": "crash-ping-ingest/1.0 daily_batches"}, method="HEAD")
    try:
        return urlopen(request).status == 200
    except:
        return False


@transforms.add
def create_daily_tasks(config, tasks):
    """Duplicates a task for each of the days that is missing and sets the
    appropriate routes.

    Only applies to `cron` jobs.
    """
    today = datetime.datetime.now(datetime.UTC).date()

    for task in tasks:
        daily_batches = task.pop('cron-daily-batches', None)
        if daily_batches is None or config.params["tasks_for"] != "cron":
            yield task
            continue

        days = daily_batches["days"]
        index = daily_batches.get("index")
        env = daily_batches.get("env")

        # We only create tasks for complete days: `days` will immediately
        # *precede* `today`.
        for preceding in range(-days, 0):
            daily_task = deepcopy(task)
            date = today + datetime.timedelta(days = preceding)

            daily_task["name"] += str(preceding)

            if index is not None:
                task_index = index.format(date=date)
                daily_task.setdefault("optimization", {}).setdefault("index-search", []).append(task_index)
                daily_task.setdefault("routes", []).append(f"index.{task_index}")

            if env is not None:
                daily_task["worker"].setdefault("env", {})[env] = str(date)

            yield daily_task


@transforms.add
def create_daily_dependencies(config, tasks):
    """Adds dependencies on tasks created by create_daily_tasks.

    Only applies to `cron` jobs.
    """
    for task in tasks:
        all_daily_deps = task.pop('cron-daily-dependencies', [])
        if len(all_daily_deps) == 0 or config.params["tasks_for"] != "cron":
            yield task
            continue

        deps = task.setdefault("dependencies", {})
        fetches = task.setdefault("fetches", {})

        for daily_deps in all_daily_deps:
            days = daily_deps["days"]
            task = daily_deps["task"]
            artifacts = daily_deps.get("artifacts", [])
            for preceding in range(-days, 0):
                key = f"create-daily-dependency-{task}{preceding}"
                deps[key] = f"{task}{preceding}"
                fetches[key] = [{"artifact": artifact, "extract": False, "dest": f"cron-daily-dependencies/{task}{preceding}"} for artifact in artifacts]

        yield task

