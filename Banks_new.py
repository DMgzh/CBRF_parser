import logging
from grab.spider import Spider, Task
from pymongo import MongoClient
import csv
import json

filials = dict()


def get_date(url):
    left_idx = url.find('web/') + 4
    right_part = url[left_idx:]
    right_idx = right_part.find('/')
    date = right_part[:right_idx]
    return date

def get_code(url):
    left_idx = url.find('id=') + 3
    return url[left_idx:]


class BankiSpider(Spider):
    initial_urls = ["https://web.archive.org/web/202101/https://www.cbr.ru/banking_sector/credit/coinfo/?id=190000011"]

    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        self.db = self.client["Sources"]
        self.collection = self.db["Banki"]
        self.url = "https://www.banki.ru/"
        

    def task_initial(self, grab, task):
        with open("cb parser/data/DATES1.csv") as fp:
            reader_dates = csv.reader(fp, delimiter=",", quotechar='"')
            dates = [row for row in reader_dates]
            
        with open("cb parser/data/IDS.csv") as gp:
            reader_ids = csv.reader(gp, delimiter=",", quotechar='"')
            ids = [row for row in reader_ids]

        for id in ids:
            for date in dates:

                print(id[0])
                
                yield Task("bank",
                           url="https://web.archive.org/web/%s/https://www.cbr.ru/banking_sector/credit/coinfo/?id=%s" 
                           % (date[0], id[0]))
                     
                     
    def task_bank(self, grab, task):
        cur_url = grab.doc.url
        cur_date = get_date(cur_url)
        req_url = task.url
        req_date = get_date(req_url)
        cb_code = get_code(cur_url)
        
        
        if cb_code not in filials:
            filials[cb_code] = {}
        
        if cur_date not in filials[cb_code]:
            filials[cb_code][cur_date] = []
        
        
            page_tables = grab.doc("//table[@class='data']")
            tables_n = len(page_tables)
                    
            for i in range(1, tables_n+1):
                table = grab.doc(f"(//table[@class='data'])[{i}]")

                if table.select('.//tr[1]').select('.//th[1]').text() in ['Регистрационный номер',
                                                                          'рег.н.',
                                                                          'рег. №, присвоенный Банком России']:
                    
                    rows_n = len(table.select('.//tr'))
                    cols_n = len(table.select('.//tr[2]').select('.//td'))
                    
                    for row in range(2, rows_n+1):
                        filials[cb_code][cur_date].append({})
                        
                        for col in range(1, cols_n+1):
                            filials[cb_code][cur_date][-1][table.select(f'.//tr[1]').select(f'.//th[{col}]').text()] \
                                = table.select(f'.//tr[{row}]').select(f'.//td[{col}]').text()
                        filials[cb_code][cur_date][-1]["Место нахождения (фактический адрес)"] \
                            = filials[cb_code][cur_date][-1]["Место нахождения (фактический адрес)"].split(', ')

                    break
        

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

with open('output.json', 'w') as fout:
    json.dump(filials, fout, indent=4, ensure_ascii=False)
