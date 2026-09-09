from taskgraph.target_tasks import register_target_task

@register_target_task("daily_pings")
def target_tasks_daily_pings(full_task_graph, parameters, graph_config):
    """
    Select the task for running the daily ping ingestion.
    """
    for name, task in full_task_graph.tasks.items():
        if name.startswith("process-pings-"):
            yield name


@register_target_task("live_pings")
def target_tasks_live_pings(full_task_graph, parameters, graph_config):
    """
    Select the task for running ingestion of live pings.
    """
    yield "process-live-pings"
