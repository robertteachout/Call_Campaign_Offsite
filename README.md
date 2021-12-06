# Call Center Campaign - Offsite Retrieval Method

## Objective
The goal of this project is to automate the daily loading of call campaigns. 

### Process:
#### Daily production steps
00. Create/track a 10-day sprint schedule
01. Load raw zip file
02. Load addition inventory from server
03. Clean & standardize data
04. Separate inventory into skills aka teams of employees
05. Test inventory is complete and up to date
------
06. Daily mapping on sprint schedule
07. New Inventory load balance across remaining sprint
08. Last call inventory search on the database
09. Identify rolled inventory from previous days sprint
10. Map priority based on steps 6-9
11. Split inventory by skill, group by contact id (phone#s), and score (rank order to called)
12. Append new inventory to master sprint schedule
------
13. Pivot table progress tracker 
14. Save & upload information to the cloud
15. Insert campaign into server database

#### New Sprint schedule
0. Track 10-day sprint
1. Find next 10 business days, CIOX custom holiday calendar
2. Find unique phone #'s from the current campaign 
3. Sort by project audit type
4. Create 5 & 10-day sprints based on audit type inventory
5. Split by skill to assign individual campaigns
6. Run daily production

#### Reporting
0. Obeya weekly tracking on campaign results
1. Campaign phone# fall out flow chart
2. Call volume historical analysis


## Setup
This project was created and used with anaconda
Assumption: 
    conda is already installed and base env is configured

### Steps
0. open cmd & navigate to project root
1. Run command setup.bat
    - activates conda base env 
    - runs conda create environment.yml 
    - switches to new env

## Run program
0. Drop ASM zip file into .\data\extract
1. Open cmd & navigate to project root
2. log in to CIOX VPN
3. Run command run.bat

### Optional & highly recommended:
Setup PowerAutomate email attachments transfer 
- ASM email to .\data\extract

Setup Task Scheduler
- Native Windows application
- Create "Basic Task" with time trigger pointing at run.bat