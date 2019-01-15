import pymongo
import requests
from bs4 import BeautifulSoup
import pandas as pd
from fake_useragent import UserAgent

ua = UserAgent()
cilent = pymongo.MongoClient('localhost', 27017)
lianjia = cilent['lianjia']
missing_house_collection = lianjia['missing_url']
house_id_collection = lianjia['house_id']
data = pd.DataFrame(list(missing_house_collection.find()))
missing_house_collection.remove()
for url in list(data['missinng_url']):
    response = requests.get(url, headers={'User-Agent': str(ua.random)}).text
    soup = BeautifulSoup(response, 'lxml')
    house_num_test = soup.select('div.resultDes.clear > div.total.fl > span')
    if int(house_num_test[0].get_text()) != 0:
        for house_url in soup.select("div.info > div.title > a"):
            house_id = house_url['href'].split('.html')[0].split('/')[-1]
            house_id_collection.insert_one({'house_id': house_id})
    else:
        missing_house_collection.insert_one({'missinng_url': url})
