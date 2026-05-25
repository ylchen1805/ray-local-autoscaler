# autoscaler/policy.py
from abc import ABC, abstractmethod
import time
import yaml
from typing import Optional


class BasePolicy(ABC):
    """Abstract class for scaling policies.

    All custom policies must inherit from this class and implement three methods.

    class MyPolicy(BasePolicy):
        def should_scale_up(...):
            ...
        def should_scale_down(...):
            ...
        def records_scale(...):
            ...
    """

    @abstractmethod
    def should_scale_up(self, pending: int, alive_workers: int) -> bool:
        "Return True if the cluster should scale up."
        pass

    @abstractmethod
    def should_scale_down(
        self,
        cpu_usage: float,
        gpu_usage: Optional[float],
        pending: int,
        alive_workers: int,
    ) -> bool:
        "Return True if the cluster should scale down."
        pass

    @abstractmethod
    def records_scale(self, pending: int, alive_workers: int) -> bool:
        "Return True if the cluster should scale down."
        pass


class ThresholdPolicy(BasePolicy):
    """Threshold-base scaling policy.

    Providing thresholds in scale_policy.yaml to set the threshold.
    Or using default value."""

    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 4,
        cooldown: int = 15,
        cpu_scale_down_threshold: float = 0.1,
        gpu_scale_down_threshold: float = 0.1,
        scale_down_threshold: int = 5,
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.cpu_scale_down_threshold = cpu_scale_down_threshold
        self.gpu_scale_down_threshold = gpu_scale_down_threshold
        self.cooldown = cooldown
        self.scale_down_threshold = scale_down_threshold
