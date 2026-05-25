# jobs/test_monitor.py
from autoscaler.monitor import *

status = get_cluster_status()
print("Number of alive nodes :", len(get_alive_nodes()))
print("Number of pending resource demands :", get_pending_resources(status=status))
print("CPU usage :", get_cpu_usage(status=status))
print("GPU usage :", get_gpu_usage(status=status))
