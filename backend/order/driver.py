import random
import uuid

import ray
from pydantic import BaseModel, Field

_SURNAMES = [
    "王",
    "林",
    "陳",
    "張",
    "李",
    "黃",
    "吳",
    "劉",
    "蔡",
    "楊",
    "許",
    "鄭",
    "謝",
    "郭",
    "洪",
    "邱",
    "曾",
    "廖",
    "賴",
    "徐",
]
_GIVEN_NAMES = [
    "大明",
    "小美",
    "志豪",
    "雅惠",
    "俊賢",
    "淑芬",
    "建國",
    "文雄",
    "秀蘭",
    "志偉",
    "宜珍",
    "家豪",
    "雅婷",
    "冠宇",
    "淑惠",
    "宗翰",
    "佳穎",
    "柏翰",
    "怡君",
    "育誠",
]
_PLATE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ"


class Driver(BaseModel):
    driver_id: str
    driver_name: str
    driver_rating: float
    license_plate: str


@ray.remote
class DriverPool:
    def __init__(self, initial_size: int = 5):
        self._idle: list[Driver] = [self._gen_driver() for _ in range(initial_size)]
        self._busy: dict[str, Driver] = {}

    def acquire(self) -> Driver:
        """Take an idle driver; auto-generate one if none are available."""
        driver = self._idle.pop() if self._idle else self._gen_driver()
        self._busy[driver.driver_id] = driver
        return driver

    def release(self, driver_id: str) -> None:
        """Return a driver to the idle pool after a trip completes."""
        driver = self._busy.pop(driver_id, None)
        if driver is not None:
            self._idle.append(driver)

    def status(self) -> dict:
        return {"idle": len(self._idle), "busy": len(self._busy)}

    def _gen_driver(self) -> Driver:
        letters = "".join(random.choices(_PLATE_CHARS, k=3))
        digits = "".join(random.choices("0123456789", k=4))
        return Driver(
            driver_id=f"driver-{uuid.uuid4().hex[:6]}",
            driver_name=random.choice(_SURNAMES) + random.choice(_GIVEN_NAMES),
            driver_rating=round(random.uniform(4.0, 5.0), 1),
            license_plate=f"{letters}-{digits}",
        )
