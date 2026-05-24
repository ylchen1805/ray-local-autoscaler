# jobs/test_monitor.py
from autoscaler.monitor import *

status = get_cluster_status()
print("Number of alive nodes :", len(get_alive_nodes()))
print("Number of pending resource demands :", get_pending_resources(status=status))
