from importlib import import_module

def register(graph_config):
    """Setup taskgraph extensions"""
    # Import modules to trigger decorators
    import_module("local_taskgraph.date_tasks")
