# TO DO:
# Done 1) Merge keys by month_value in output.json and output3.json (better to rename them)
# Done 2) Merge output.json and output3.json into one file 

# UPD: 
# Done 1) Parse a table that converts reg_index to reg
# Done 2) For each date in new_cb_dict make it's values the number of filials in each region

# 3) Go through each date and each regn that are both in bankiru_regn.csv and cb_regn.csv and get for the bank:
#     - Trust rating from bankiru.json
# Done     - Filials number from output_merged.json (if there is no that bank, he has 0 filials)
# 4) Export csv

import json
import csv

def merge_dicts(d1, d2):
    d3 = {}
    
    for key in d1:
        if key in d2:
            d3[key] = d1[key] | d2[key]
        
        else:
            d3[key] = d1[key]
    
    for key in d2:
        if key not in d3:
            d3[key] = d2[key]

    return d3
    
    
def merge_months(cb_dict):
    new_cb_dict = {}
    for cb_id in cb_dict:
        new_cb_dict[cb_id] = {}
        
        for date in cb_dict[cb_id]:
            short_date = date[:6]
            
            if short_date not in new_cb_dict[cb_id]:
                new_cb_dict[cb_id][short_date] = []
            
            if len(cb_dict[cb_id][date]) > len(new_cb_dict[cb_id][short_date]):
                new_cb_dict[cb_id][short_date] = cb_dict[cb_id][date]
            
    return new_cb_dict

# Объединяем месяца в файлах
with open('cb_problems_1.json', 'r') as fin1:
    cb_dict_1 = json.load(fin1)
    new_cb_dict_1 = merge_months(cb_dict_1)

with open('cb_problems_2.json', 'r') as fin2:
    cb_dict_2 = json.load(fin2)
    new_cb_dict_2 = merge_months(cb_dict_2)

# Объединяем файлы
new_cb_dict = merge_dicts(new_cb_dict_1, new_cb_dict_2)

    
# Достаем индексы регионов
with open('regions.csv', 'r', encoding='Windows-1251') as fin:
    reader = csv.DictReader(fin, delimiter=';')

    index_dict = {}
    for region in reader:
        for index in region['Почта'].split(','):
            index_dict[index] = region['Конституция']

# Заменяем филиалы на регионы
cb_reg_dict = {}
for cb_id in new_cb_dict:

    cb_reg_dict[cb_id] = {}
    for date in new_cb_dict[cb_id]:
        
        cb_reg_dict[cb_id][date] = {}
        
        if new_cb_dict[cb_id][date] == []:
            cb_reg_dict[cb_id][date] = None
            continue 
        
        for filial in new_cb_dict[cb_id][date]:
            adress = filial['Место нахождения (фактический адрес)']
            
            fil_index = adress[0][:3]
            
            if not fil_index.isnumeric():
                fil_index = adress[1][:3]
            
            if fil_index not in index_dict:
                if adress[0] == 'Республика Крым':
                    
                    region = '92'
            
                    if region not in cb_reg_dict[cb_id][date]:
                        cb_reg_dict[cb_id][date][region] = 0 
                    cb_reg_dict[cb_id][date][region] += 1
                    continue
                
                elif adress[1] == 'Республика Башкортостан':
                    fil_index = adress[2][:3]
                
                elif adress[0] == 'г.Москва':
                    
                    region = '77'
            
                    if region not in cb_reg_dict[cb_id][date]:
                        cb_reg_dict[cb_id][date][region] = 0
                    cb_reg_dict[cb_id][date][region] += 1
                    continue
                    
                else:
                    print(adress)
                    continue
            
            region = index_dict[fil_index]
            
            if region not in cb_reg_dict[cb_id][date]:
                cb_reg_dict[cb_id][date][region] = 0
            cb_reg_dict[cb_id][date][region] += 1

print(cb_reg_dict)

with open('cb_problems_filials.json', 'w') as fout:
    json.dump(cb_reg_dict, fout, indent=4, ensure_ascii=False)      
        

# Считываем регистрационные номера из банков.ру и из ЦБ
# with open('cb_regn.csv', 'r') as fin1:
#     cb_reader = csv.DictReader(fin1, delimiter=',')
#     cb_code_dict = {}
    
#     for bank in cb_reader:
#         cb_code_dict[bank['regn']] = bank['cb_code']

# with open('bankiru_regn2.csv', 'r') as fin2:
#     bankiru_reader = csv.DictReader(fin2, delimiter=',')

# for regn in cb_code_dict:
#     cb_code = cb_code_dict[regn]
    
#     if cb_code not in cb_reg_dict:
#         cb_reg_dict[regn] = {}
#         continue
    
#     value = cb_reg_dict[cb_code]
#     cb_reg_dict.pop(cb_code)
#     cb_reg_dict[regn] = value
    
# with open('pre_final228.json', 'w') as fout:
#     json.dump(cb_reg_dict, fout, indent=4, ensure_ascii=False)
