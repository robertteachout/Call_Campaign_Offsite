import json
from dataclasses import dataclass

@dataclass
class business_lines:
    projects: list
    capacity: int
    system:str
    skill:str

def ciox_busines_lines() -> list[business_lines]: 
    with open('data/table_drop/business_lines.json',) as json_file:
        data = json.load(json_file)
        return [business_lines( *d.values() ) for d in data]

