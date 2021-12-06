# Call Center Campaign - Offsite Retrieval Method

## Objective
The goal of this project is to automate daily loading & tracking of call campaigns. 

## Process:
### Daily Campaign Transformation
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

### New Sprint schedule
0. Track 10-day sprint
1. Find next 10 business days, CIOX custom holiday calendar
2. Find unique phone #'s from the current campaign 
3. Sort by project audit type
4. Create 5 & 10-day sprints based on audit type inventory
5. Split by skill to assign individual campaigns

### Reporting
0. Obeya weekly tracking on campaign results
1. Campaign phone# fall out flow chart
2. Call volume historical analysis

## Setup
Assumption: conda is already installed
(https://www.anaconda.com/products/individual)

Open cmd, navigate to the project root & run command
```cmd
setup.bat
```
This file:
- activates conda base env 
- runs conda create environment.yml 
- switches to new env

## Run program
0. Drop ASM zip file into .\data\extract
1. Open cmd & navigate to project root
2. Log in to CIOX VPN
3. Run
```cmd
run.bat
```

## Optional & highly recommended:
### PowerAutomate email attachments transfer 
(https://us.flow.microsoft.com/en-us/)
- ASM email to .\data\extract

### Task Scheduler 
(https://www.windowscentral.com/how-create-automated-task-using-task-scheduler-windows-10)
- Native Windows application
- Create "Basic Task" with time trigger pointing at run.bat