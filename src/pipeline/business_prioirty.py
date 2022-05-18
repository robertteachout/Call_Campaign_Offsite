import json
import os
from dataclasses import dataclass, field
from typing import Dict, List

from .tables import CONFIG_PATH
from .utils import Business_Days

bus_day = Business_Days()

@dataclass
class Business_Line:
    skill: str              = field(default_factory=lambda: "New_skill")
    filters: Dict[str,list] = field(default_factory=lambda:{
                                                            "Audit_Type":[],
                                                            "Project_Type":[],
                                                            "Outreach_Status":[],
                                                            "general":[]
                                                            })
    new_columns: List       = field(default_factory=lambda: [])
    scoring:Dict[str,bool]  = field(default_factory=lambda:{"meet_target_sla":True, 
                                                            "no_call":False,  
                                                            "age":False})

    def as_dict(self):
        return {'skill': self.skill, 'filters': self.filters, 'scoring': self.scoring}

def read_json(name):
    with open(CONFIG_PATH / name) as json_file:
            return json.load(json_file)

def write_json(output):
    with open(CONFIG_PATH / f"{bus_day.today_str}.json",'w') as file:
        json.dump(output, file, indent=4)

def company_busines_lines() -> list[Business_Line]:
    try:
        custom_skills = os.listdir(CONFIG_PATH / "custom_skills")
        data = read_json(f"custom_skills/{max(custom_skills)}")
        new_skill = Business_Line()
        bus = [Business_Line(*d.values()) for d in data]
        bus.append(new_skill)
        return bus
    except:
        return None
