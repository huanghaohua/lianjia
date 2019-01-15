import requests
from bs4 import BeautifulSoup
import pymongo
from fake_useragent import UserAgent
import time
import pandas as pd


ua = UserAgent()
Mymongo = pymongo.MongoClient('localhost', 27017)
lianjia = Mymongo['lianjia']
house_id_collection = lianjia['house_id']
region_url_collection = lianjia['region_url']
missing_url_collection = lianjia['missing_url']
base_url = "https://gz.lianjia.com/chengjiao/"


def get_house_id(region_url):
    response = requests.get(region_url, headers={'User-Agent': str(ua.random)}).text
    soup = BeautifulSoup(response, 'lxml')
    house_num = soup.select('div.total.fl > span')
    # page_num = int(int(house_num[0].get_text())/30)
    page_num = int(int(house_num[0].get_text())/30)+1 if int(house_num[0].get_text()) > 30 else 1  # 获取页码
    for i in range(1, page_num+1):
        time.sleep(1)
        current_url = region_url + 'pg{}'.format(i)  # 拼接URL
        response = requests.get(current_url, headers={'User-Agent': str(ua.random)}).text
        soup = BeautifulSoup(response, 'lxml')
        house_num_test = soup.select('div.resultDes.clear > div.total.fl > span')
        if int(house_num_test[0].get_text()) != 0:
            for house_url in soup.select("div.info > div.title > a"):
                house_id = house_url['href'].split('.html')[0].split('/')[-1]
                house_id_collection.insert_one({'house_id': house_id})
        else:
            print(current_url)
            # missing_url_collection.insert_one(current_url)


def get_all_house_id():
    data = pd.DataFrame(list(region_url_collection.find()))
    for region_url in list(set(data['region_url'])):   # 经过观察发现有重复的地区地址，用set集合去重后再转化为list
        get_house_id(region_url)


get_all_house_id()
