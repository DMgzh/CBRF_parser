import csv 

with open('cb_regn.csv', 'r') as fin:
    reader = csv.DictReader(fin, delimiter=',')
    cb_dict = {}
    
    for row in reader:
        cb_dict[row['regn']] = row['cb_code']
        
    

with open('PROBLEMS_login.csv', 'r') as fin:
    reader = list(csv.DictReader(fin, delimiter=';'))
    
    for idx in range(len(reader)):
        if reader[idx]['REGN'] in cb_dict:
            reader[idx]['cb_code'] = cb_dict[reader[idx]['REGN']]
        else:
            print('not found', reader[idx]['REGN'])


    with open('banks_regn.csv', 'w') as fout:
        fieldnames = ['REGN', 'login', 'cb_code']
        
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reader)

    