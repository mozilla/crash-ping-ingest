from copy import deepcopy
import datetime
from urllib.request import Request, urlopen
from taskgraph.actions.registry import register_callback_action
from taskgraph.decision import taskgraph_decision
from taskgraph.parameters import extend_parameters_schema, Parameters
from taskgraph.transforms.base import TransformSequence
from taskgraph.util.schema import Schema
from voluptuous import Required, ALLOW_EXTRA

CI_INDEX_API = "https://firefox-ci-tc.services.mozilla.com/api/index/v1"

TASK_SCHEMA = Schema({
    # If specified, the task will be duplicated to create date-specific tasks.
    "date-tasks": {
        # The number of days preceding the cron run date for which to generate tasks.
        # Only applies when getting cron tasks.
        "cron-days": int,
        # Whether to create a task for the manual processing action.
        "action-manual": bool,
        # The index path to use, both to index the results and to avoid running
        # a task when one has already run. The `{date}` string will be
        # interpolated with the date, where `-`s will be replaced with `.`
        # (e.g., 2025-01-02 will become `2025.01.02`).
        "index": str,
        # An environment variable name which (if specified) will be set with the date.
        "env": str,
    },
    # If specified, dependencies corresponding to `days` tasks created by
    # `date-task.cron-days` will be added to the task.
    "cron-date-dependencies": [{
        # The number of days preceding the cron run date for which to create
        # dependencies. This should not exceed the number of days specified in
        # the corresponding `date-task.cron-days`.
        Required("days"): int,
        # The task name.
        Required("task"): str,
        # Artifacts to fetch. They will end up in `fetches/cron-date-dependencies/{task}-N`.
        "artifacts": [str],
    }],
}, extra=ALLOW_EXTRA)

PROCESS_PINGS_MANUAL_PARAM = "process_pings_manual"

PARAMETERS_SCHEMA = {
    PROCESS_PINGS_MANUAL_PARAM: {
        Required("dates"): [str],
        "index": bool
    }
}


# Transforms logic
transforms = TransformSequence()
transforms.add_validate(TASK_SCHEMA)


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
def create_date_tasks(config, tasks):
    """Duplicates a task based on the date-tasks configuration.

    The original task will never be yielded if `date-tasks` is specified.
    """
    today = datetime.datetime.now(datetime.UTC).date()

    for task in tasks:
        cfg = task.pop('date-tasks', None)
        if cfg is None:
            yield task
            continue

        days = cfg.get("cron-days")
        manual = cfg.get("action-manual")
        index = cfg.get("index")
        env = cfg.get("env")

        def set_task_date(task, datestr, add_index = True, index_search = False):
            if index is not None and add_index:
                task_index = index.format(date=datestr.replace("-", "."))
                if index_search:
                    task.setdefault("optimization", {}).setdefault("index-search", []).append(task_index)
                task.setdefault("routes", []).append(f"index.{task_index}")

            if env is not None:
                task["worker"].setdefault("env", {})[env] = datestr

        if config.params["tasks_for"] == "cron" and days is not None:
            # We only create tasks for complete days: `days` will immediately
            # *precede* `today`.
            for preceding in range(-days, 0):
                date = today + datetime.timedelta(days = preceding)
                new_task = deepcopy(task)
                new_task["name"] += str(preceding)
                set_task_date(new_task, str(date), index_search = True)
                yield new_task

        if (config.params["tasks_for"] == "action"
            and manual is not None
            and PROCESS_PINGS_MANUAL_PARAM in config.params):
            manual_cfg = config.params[PROCESS_PINGS_MANUAL_PARAM]
            dates = manual_cfg["dates"]
            add_index = manual_cfg.get("index", True)
            for date in dates:
                new_task = deepcopy(task)
                new_task["name"] += "-manual-" + date
                set_task_date(new_task, manual_cfg["date"], add_index = add_index)
                yield new_task


@transforms.add
def create_daily_dependencies(config, tasks):
    """Adds dependencies on tasks created by create_daily_tasks.

    Only applies to `cron` jobs.
    """
    for task in tasks:
        all_daily_deps = task.pop('cron-date-dependencies', [])
        if len(all_daily_deps) == 0 or config.params["tasks_for"] != "cron":
            yield task
            continue


        deps = task.setdefault("dependencies", {})
        fetches = task.setdefault("fetches", {})

        for daily_deps in all_daily_deps:
            days = daily_deps["days"]
            task_name = daily_deps["task"]
            artifacts = daily_deps.get("artifacts", [])
            for preceding in range(-days, 0):
                key = f"create-daily-dependency-{task_name}{preceding}"
                deps[key] = f"{task_name}{preceding}"
                fetches[key] = [{"artifact": artifact, "extract": False, "dest": f"cron-date-dependencies/{task_name}{preceding}"} for artifact in artifacts]

        yield task


# Action logic
extend_parameters_schema(PARAMETERS_SCHEMA)

@register_callback_action(
    name='process-pings-manual',
    title='Process Pings (Manual)',
    symbol='ppm',
    description='Manually process pings for the given days.',
    order=1,
    schema={
        'title': 'Manual Ping Processing Options',
        'description': 'Parameters to use for manual ping processing.',
        'properties': {
            'dates': {
                'title': 'Dates',
                'description': 'The dates for which to process crash pings.',
                'type': 'array',
                'items': {
                    'type': 'string',
                    'format': 'date',
                },
            },
            'index': {
                'title': 'Index',
                'description': 'Index the processed results.',
                'type': 'boolean',
                'default': 'true',
            },
        },
        'required': ['date'],
        'additionalProperties': False,
    }
)
def process_pings_manual_action(parameters, graph_config, input, task_group_id, task_id):
    parameters = dict(parameters)
    parameters["tasks_for"] = "action"
    parameters[PROCESS_PINGS_MANUAL_PARAM] = input
    taskgraph_decision({"root": graph_config.root_dir}, Parameters(**parameters))
