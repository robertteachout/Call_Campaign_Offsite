import json
from dataclasses import dataclass

from .tables import CONFIG_PATH


@dataclass
class business_lines:
    projects: list
    capacity: int
    system: str
    skill: str


def ciox_busines_lines() -> list[business_lines]:
    with open(
        CONFIG_PATH / "business_lines.json",
    ) as json_file:
        data = json.load(json_file)
        return [business_lines(*d.values()) for d in data]
