import requests
from bs4 import BeautifulSoup
import json

url = 'https://www.banki.ru/services/responses/?date=01.05.2021'

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

a = dict(json.loads(response))
print(a)
with open('banki_ru_info.json', 'w') as fout:
    json.dump(a, fout, indent=4)

