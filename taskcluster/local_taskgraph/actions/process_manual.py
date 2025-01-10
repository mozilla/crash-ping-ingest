import copy
from taskgraph.actions.registry import register_callback_action
from taskgraph.create import create_tasks
from taskgraph.generator import TaskGraphGenerator
from taskgraph.taskgraph import TaskGraph

def add_ping_submission_date(task, taskgraph, date, index):
    if task.label != "process-pings":
        return
    task.task["payload"]["env"]["PING_SUBMISSION_DATE"] = date
    if index:
        task.task.setdefault("routes", []).append(f"index.mozilla.v2.crash-ping-ingest.{date}.processed")

@register_callback_action(
    name='process-pings-manual',
    title='Process Pings (Manual)',
    symbol='ppm',
    description='Manually process pings for a given day.',
    order=1,
    schema={
        'title': 'Manual Ping Processing Options',
        'description': 'Parameters to use for manual ping processing.',
        'properties': {
            'date': {
                'title': 'Date',
                'description': 'The date for which to process crash pings.',
                'type': 'string',
                'format': 'date',
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
    date = input["date"]
    index = input.get("index", True)
    generator = TaskGraphGenerator(None, parameters)

    graph = generator.morphed_task_graph
    graph.for_each_task(add_ping_submission_date, date, index)

    create_tasks(
        generator.graph_config,
        graph,
        generator.label_to_taskid,
        generator.parameters,
        task_group_id
    )
