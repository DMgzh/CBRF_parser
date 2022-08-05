# TO DO:
# 1) func or method to find of the bank is already in memoryBook - DONE
# 2) apply (1) to task_bank() - DONE 
# 3) export csv - DONE


import requests
import logging
from grab.spider import Spider, Task
from pymongo import MongoClient
import csv
import json

def get_league(agentId, banks_list):
    for bank_idx in range(len(banks_list)):
        if banks_list[bank_idx]['agentId'] == agentId:
            return banks_list[bank_idx]['leagueCode']


### globals ###
fieldnames = ['regn', 'url', 'code', 'agentId']
url_list = []

checked_banks = set()

### parser ###
class BankiSpider(Spider):
    initial_urls = ['https://www.banki.ru/banks/bank/aleksandrovsky']
    
    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        self.db = self.client["Sources"]
        self.collection = self.db["Banki"]
        self.url = "https://www.banki.ru/"
        
        self.output = []

    def task_initial(self, grab, task):
        with open('bankiru1.json', 'r') as fin:
            data = json.load(fin)
                        
            # Проход по данным
            for date in data:
                date_data = data[date]["ratingSuggestData"]
                for bank_info in date_data:
                    
                    bank_id = str(bank_info["agentId"])
                    bank_league = bank_info['leagueCode']
                    bank_code = data[date]['banksData'][bank_id]['code']

                    # Исключаем повторение парсинга одного и того же банка
                    if bank_code in checked_banks:
                        continue
                    
                    # Парсинг
                    yield Task("bank",
                                url=f"https://www.banki.ru/banks/bank/{bank_code}",
                                bank_code=bank_code,
                                bank_league=bank_league,
                                bank_id=bank_id,
                                )
                    # Добавляем в множество спаршенных банков
                    checked_banks.add(bank_code)

        
    
    def task_bank(self, grab, task):            
        try:
            regn = grab.doc('(//span[@class="text-size-6 color-minor-black-lighten text-nowrap margin-right-small"])[1]').text()  
            regn = regn.split(' № ')[1]
        # Проверить, подходит ли версия сайта              
        except IndexError:
            try:
                regn = grab.doc("(//dd[@data-test='memory-book-bank-license'])[1]").text()
                regn = regn.split(' | ')[0]
            except IndexError:
                print('no url found for bank code:', task.bank_code)
                return
                
        url_list.append({'code': task.bank_code, 'regn': regn, 'url': task.url, 'agentId': task.bank_id})


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    bot = BankiSpider()
    bot.setup_cache(
        backend="mongodb",
        port=27017,
        host="localhost",
        database="Sources",
        username="admin",
        password="qwerty",
    )
    bot.run()

    # Вывод и запись в файл
    for bank in url_list:
        print(bank)

    print(len(url_list))

    with open('bankiru_regn2.csv', 'w') as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(url_list)
