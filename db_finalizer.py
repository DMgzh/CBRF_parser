# TO DO:
#     Done 1) Work with bankiru.json:
#         - Change agentId to regn and delete all the info but for the rating
#         - Make new json with structure Regn - Date - Rating
#     Done 2) Change the date format in pre_final.json
#     3) For each available regn in pre_final add filials info to new json made in (1)
#     4) Export
import json
import csv
import re

regn_dict = {}
with open('bankiru_regn2.csv', 'r') as fin:
    reader = csv.DictReader(fin, delimiter=',')
    for line in reader:
        regn_dict[line['agentId']] = line['regn']


bankiru_dict = {}
for agent_id in regn_dict:
    regn = regn_dict[agent_id]
    
    bankiru_dict[regn] = {}

not_found = set()
with open('bankiru1.json', 'r') as fin:
    bankiru = json.load(fin)

    for date in bankiru:
        new_date = date.split('.')[2] + date.split('.')[1]
        
        for regn in bankiru_dict:
            bankiru_dict[regn][new_date] = ['N/A']
        
        for bank in bankiru[date]['ratingData']:
            if str(bank['agentId']) not in regn_dict:
                not_found.add(str(bank['agentId']))
                continue
                
            regn = regn_dict[str(bank['agentId'])]
            rating = bank['rating']
            
            bankiru_dict[regn][new_date] = [rating]


with open('pre_final228.json', 'r') as fin:
    filials = json.load(fin)
    
    for regn in bankiru_dict:
        
        if regn in filials:
            for date in bankiru_dict[regn]:
                
                if date in filials[regn]:
                    bankiru_dict[regn][date].append(filials[regn][date])
        else:
            print(regn)

with open('final228.json', 'w') as fout:
    json.dump(bankiru_dict, fout, indent=4, ensure_ascii=False)
