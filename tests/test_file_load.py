import sys
import os
import pytest
from datetime import date
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

import src.pipeline.clean
import src.pipeline.skills
import src.pipeline.sprint_schedule
from src.pipeline.tables import zipfiles, tables

today = '2021-12-06'
tomorrow = '2021-12-07'
path = Path('tests/test_data')

def test_check_day():
    start = tables('pull', 'NA', 'start.csv', path)
    Day, Master_List = src.pipeline.sprint_schedule.check_day(start, tomorrow)
    assert Day == 6 
    assert Master_List == 1

@pytest.fixture(scope="module")
def load_campaign():
    today = '06'
    filename = str(f'Call_Campaign_v4_12{today}*')
    load = zipfiles('pull', 'NA', filename, extract=path)
    return load

def test_file_clean(load_campaign):
    tomorrow = '2021-12-07'
    clean = src.pipeline.clean.clean(load_campaign, tomorrow)
    assert len(clean.OutreachID) == 112398

def test_skill_tier(load_campaign):
    # print(len(load_campaign.OutreachID))
    tomorrow = '2021-12-07'
    clean = src.pipeline.clean.clean(load_campaign, tomorrow)
    _3_Bus_Day_ago = date(2021,12,2)
    skilled = src.pipeline.skills.complex_skills(clean, _3_Bus_Day_ago)
    tier1 = skilled[skilled['Skill'] == 'CC_Tier1']
    assert len(tier1.OutreachID) == 4747

