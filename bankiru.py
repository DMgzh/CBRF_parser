import requests
import logging
from grab.spider import Spider, Task
from pymongo import MongoClient
import csv
import json

filials = list()
to_dump = {}

def get_date(url):
    left_idx = url.find('date=') + 5
    date = url[left_idx:]
    return date

def get_name(url):
    left_idx = url.find('id=') + 3
    return url[left_idx:]


class BankiSpider(Spider):
    initial_urls = ["https://web.archive.org/web/201703/https://www.cbr.ru/banking_sector/credit/coinfo/?id=800000002"]
    url_dict = {}
    
    def prepare(self):
        self.client = MongoClient(host="localhost", port=27017)
        self.db = self.client["Sources"]
        self.collection = self.db["Banki"]
        self.url = "https://www.banki.ru/"
        

    def task_initial(self, grab, task):
        with open("cb parser/bankiru_input/BRdates.csv") as fp:
            reader_dates = csv.reader(fp, delimiter=",", quotechar='"')
            dates = [row for row in reader_dates]
            
        leagues = {'top': '',
                   '1': 'mode=first&',
                   '2': 'mode=second&',
                   'qual': 'mode=selectiveTour&',
                   'arch': 'mode=memoryBook&'}
        
        for date in dates:
            for league in leagues:
                # В первом аргументе подставляется постфикс функции которая будет обрабатывать ссылку переданную в url
                yield Task("bank", url="https://www.banki.ru/services/responses/?%sdate=%s"
                    % (leagues[league], date[0]), league=league)
                     
                     
    def task_bank(self, grab, task):        
        url = grab.doc.url
        
        date = get_date(url)
        
        response = requests.get(url).text

        req_str = "new BanksRating('#banks-rating-container',"
        left_idx = response.find(req_str) + len(req_str)

        right_idx = response.find("averageRating:")

        response = response[left_idx:right_idx].replace('\t', '')
        response = response.replace('\n', '')
        response = response.replace('ratingData', '"ratingData"')
        response = response.replace('ratingSuggestData', '"ratingSuggestData"')
        response = response.replace('banksData', '"banksData"')

        response = response[1:-1] + '}'
        a = json.loads(response)
        
        if date not in to_dump:
            to_dump[date] = {}
            to_dump[date]["ratingData"] = []
            
            to_dump[date]["ratingSuggestData"] = a["ratingSuggestData"]
            to_dump[date]["banksData"] = a["banksData"]
            
        to_dump[date]["ratingData"] += a['ratingData']
        
        
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

    with open('bankiru_final.json', 'w') as fout:
        json.dump(to_dump, fout, indent=4, ensure_ascii=False)
        