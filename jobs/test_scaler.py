# jobs/test_scaler.py
from autoscaler.scaler import scale_down, scale_up
import sys

if len(sys.argv) < 2:
    scale_up(1)
elif sys.argv[1] == "up":
    scale_up(1)
elif sys.argv[1] == "down":
    scale_down(1)
