import requests
import logging
from grab.spider import Spider, Task
from pymongo import MongoClient
import csv
import json

filials = list()


def get_date(url):
    left_idx = url.find('web/') + 4
    right_part = url[left_idx:]
    right_idx = right_part.find('/')
    date = right_part[:right_idx]
    return date

def get_name(url):
    left_idx = url.find('id=') + 3
    return url[left_idx:]


class BankiSpider(Spider):
    initial_urls = ["https://web.archive.org/web/20150329194441/http://cbr.ru/credit/coinfo.asp?id=710000047"]
    url_dict = {}
    to_dump = {}
    
    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        self.db = self.client["Sources"]
        self.collection = self.db["Banki"]
        self.url = "https://www.banki.ru/"
        

    def task_initial(self, grab, task):
        with open("cb_parser/data/DATES.csv") as fp:
            reader_dates = csv.reader(fp, delimiter=",", quotechar='"')
            dates = [row for row in reader_dates]
            
        with open("problems_info.csv") as gp:
            reader = csv.DictReader(gp, delimiter=",", quotechar='"')
            ids = [row['cb_code'] for row in reader]
        #list = [ids[index] for index in range(148,150,1)]
        #print(list)
        #dates = [['202202']]
        #ids = [['800000002']]
        for id in ids:
            print(id)
            for date in dates:
        # В первом аргументе подставляется постфикс функции которая будет обрабатывать ссылку переданную в url
                print(id)
                
                yield Task("bank", url="https://web.archive.org/web/%s/http://cbr.ru/credit/coinfo.asp?id=%s"
                   % (date[0], id))
                     
                     
    def task_bank(self, grab, task):
        cur_url = grab.doc.url
        cur_date = get_date(cur_url)
        req_url = task.url
        req_date = get_date(req_url)
        bank_name = get_name(cur_url)
        
        
        if bank_name not in self.to_dump:
            self.to_dump[bank_name] = {}
        
        if cur_date not in self.to_dump[bank_name]:
            self.to_dump[bank_name][cur_date] = []
        
        
            page_tables = grab.doc("//table[@class='data']")
            tables_n = len(page_tables)
                    
            for i in range(1, tables_n+1):
                table = grab.doc(f"(//table[@class='data'])[{i}]")

                if table.select('.//tr[1]').select('.//th[1]').text() in ['Регистрационный номер', 'рег.н.', 'рег. №, присвоенный Банком России']:
                    
                    rows_n = len(table.select('.//tr'))
                    cols_n = len(table.select('.//tr[2]').select('.//td'))
                    
                    for row in range(2, rows_n+1):
                        self.to_dump[bank_name][cur_date].append({})
                        
                        for col in range(1, cols_n+1):
                            self.to_dump[bank_name][cur_date][-1][table.select(f'.//tr[1]').select(f'.//th[{col}]').text()] = table.select(f'.//tr[{row}]').select(f'.//td[{col}]').text()
                        self.to_dump[bank_name][cur_date][-1]["Место нахождения (фактический адрес)"] = self.to_dump[bank_name][cur_date][-1]["Место нахождения (фактический адрес)"].split(', ')

                    break
        

        # if bank_name not in self.url_dict:
        #         self.url_dict[bank_name] = []
        # self.url_dict[bank_name].append({'current_date': cur_date, 'required_date': req_date})
        
    

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

    with open('cb_problems_2.json', 'w') as fout:
        json.dump(bot.to_dump, fout, indent=4, ensure_ascii=False)

            