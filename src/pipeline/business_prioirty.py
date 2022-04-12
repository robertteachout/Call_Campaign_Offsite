import json
from dataclasses import dataclass

from .tables import CONFIG_PATH


@dataclass
class Business_Line:
    projects: list
    capacity: int
    system: str
    skill: str
    scoring:dict

def read_json():
    with open(CONFIG_PATH / "business_lines.json") as json_file:
            return json.load(json_file)

def ciox_busines_lines() -> list[Business_Line]:
    try:
        data = read_json()
        return [Business_Line(*d.values()) for d in data]
    except:
        return None


