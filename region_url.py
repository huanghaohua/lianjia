import requests
from urllib.parse import urljoin
from scrapy.selector import Selector
import pymongo
from fake_useragent import UserAgent


ua = UserAgent()
Mymongo = pymongo.MongoClient('localhost', 27017)  # 连接本地服务
lianjia = Mymongo['lianjia']   # 链接数据库
region_url_collection = lianjia['region_url']  # 集合对象
base_url = "https://gz.lianjia.com/chengjiao/"
region_list = ['tianhe', 'yuexiu', 'liwan', 'haizhu', 'panyu', 'baiyun', 'huangpugz', 'conghua', 'zengcheng', 'huadou', 'nansha']


def get_region_url():
    for i in range(0, len(region_list)):
        url = urljoin(base_url, region_list[i])  # 拼接URL
        response = requests.get(url, headers={'User-Agent': str(ua.random)}).text
        selector = Selector(text=response)
        area_url_list = selector.xpath('/html/body/div[3]/div[1]/dl[2]/dd/div/div[2]/a/@href').extract()  # 获取地址列表
        for url in area_url_list:
            region_url = urljoin(base_url, url)
            region_url_collection.insert_one({'region_url': region_url})  # 插入数据


get_region_url()

