# autoscaler/policy.py
import time
import yaml
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class BasePolicy(ABC):
    """Abstract class for scaling policies.
    All custom policies must inherit from this class and implement three methods.

    class MyPolicy(BasePolicy):
        def should_scale_up(...):
            ...
        def should_scale_down(...):
            ...
    """

    def __init__(self) -> None:
        self.cooldown = 0
        self._last_scale_time = 0.0

    def _is_cooldown(self) -> bool:
        """Return whether the policy is in cooldown."""
        return time.time() - self._last_scale_time < self.cooldown

    def records_scale(self):
        """Records the last scale operation."""
        self._last_scale_time = time.time()

    @abstractmethod
    def should_scale_up(self, pending: list, alive_workers: int) -> bool:
        """Return True if the cluster should scale up."""
        pass

    @abstractmethod
    def should_scale_down(
        self,
        cpu_usage: float,
        gpu_usage: Optional[float],
        pending: list,
        alive_workers: int,
    ) -> bool:
        """Return True if the cluster should scale down."""
        pass


class ThresholdPolicy(BasePolicy):
    """Threshold-based scaling policy.

    Providing thresholds in scale_policy.yaml to set the threshold.
    Or using default values.
    """

    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 4,
        cooldown: int = 15,
        cpu_scale_down_threshold: float = 0.1,
        gpu_scale_down_threshold: Optional[float] = None,
        scale_up_threshold: int = 5,
    ):
        super().__init__()
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.cpu_scale_down_threshold = cpu_scale_down_threshold
        self.gpu_scale_down_threshold = gpu_scale_down_threshold
        self.cooldown = cooldown
        self.scale_up_threshold = scale_up_threshold
        self._pending_count = 0

    def should_scale_up(self, pending: list, alive_workers: int) -> bool:
        """Return True if the cluster should scale up."""
        if pending:
            self._pending_count += 1
        else:
            self._pending_count = 0
            return False

        if self._is_cooldown():
            return False

        return (
            alive_workers < self.max_workers
            and self._pending_count >= self.scale_up_threshold
        )

    def should_scale_down(
        self,
        cpu_usage: float,
        gpu_usage: Optional[float],
        pending: list,
        alive_workers: int,
    ) -> bool:
        """Return True if the cluster should scale down."""
        if pending or alive_workers <= self.min_workers:
            return False

        if self._is_cooldown():
            return False

        if cpu_usage > self.cpu_scale_down_threshold:
            return False

        if self.gpu_scale_down_threshold is not None:
            if gpu_usage is None or gpu_usage > self.gpu_scale_down_threshold:
                return False

        return True


_POLICY_REGISTRY = {
    "threshold": ThresholdPolicy,
}


def load_policy(path: str = "scale_policy.yaml") -> BasePolicy:
    """Load a scaling policy from a YAML config file.

    If the file is not found, returns a default ThresholdPolicy.
    """
    config_path = Path(path)

    if not config_path.exists():
        print("[policy] scale_policy.yaml not found, using default ThresholdPolicy.")
        return ThresholdPolicy()

    config = yaml.safe_load(config_path.read_text())
    name = config.pop("name", "threshold")

    if name not in _POLICY_REGISTRY:
        available = list(_POLICY_REGISTRY.keys())
        raise ValueError(f"Unknown policy: '{name}'. Available: {available}")

    return _POLICY_REGISTRY[name](**config)
