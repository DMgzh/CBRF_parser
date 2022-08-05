# TO DO:
# 1) get cb_id for all banks
# 2) parse regn for each cb_id
# 3) export csv


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
fieldnames = ['regn', 'cb_code']
url_list = []

checked_banks = set()

### parser ###
class BankiSpider(Spider):
    initial_urls = ['https://www.cbr.ru/banking_sector/credit/coinfo/?id=350000008']
    
    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        self.db = self.client["Sources"]
        self.collection = self.db["Banki"]
        self.url = "https://www.banki.ru/"
        
        self.output = []

    def task_initial(self, grab, task):
        with open("cb parser/data/IDS.csv") as fin:
            data = fin.read().splitlines()
                   
            # Проход по данным
            for cb_code in data:
                
                # Исключаем повторение парсинга одного и того же банка
                if cb_code in checked_banks:
                    continue
                
                # Парсинг
                yield Task("bank",
                            url=f"https://www.cbr.ru/banking_sector/credit/coinfo/?id={cb_code}",
                            cb_code=cb_code
                            )
                # Добавляем в множество спаршенных банков
                checked_banks.add(cb_code)

    
    def task_bank(self, grab, task):            
        try:
            regn = grab.doc("(//div[@class='coinfo_item_text col-md-13 offset-md-1'])[3]").text() 
            
        # Проверить, подходит ли версия сайта              
        except IndexError:
            print('-'*20)
            print(task.cb_code)
            print('-'*20)
            return
        
        url_list.append(
            {'cb_code': task.cb_code,
             'regn': regn,
             }
            )
        
        
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
    print(len(url_list))

    with open('cb_regn.csv', 'w') as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(url_list)
