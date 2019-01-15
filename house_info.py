import requests
from bs4 import BeautifulSoup
from scrapy.selector import Selector
import pymongo
from fake_useragent import UserAgent
import pandas as pd
import time
from multiprocessing import Pool


ua = UserAgent()
cilent = pymongo.MongoClient('localhost', 27017)  # 数据库连接
lianjia = cilent['lianjia']   # 数据库连接对象
house_info_collection = lianjia['house_info']  # 数据集合对象
house_url_collection = lianjia['house_url']
missing_house_info = lianjia['missing_house_info']
house_url_collection_success = lianjia['house_url_collection_success']
data = pd.DataFrame(list(house_url_collection.find()))  # 使用pandas的DataFrame结构提取出来
proxy = [
    'HTTPS://111.176.28.176:9999',
    'HTTPS://119.101.117.114:9999',
    'HTTPS://119.101.118.115:9999',
    'HTTPS://119.101.116.219:9999',
    'HTTPS://119.101.113.185:9999',
    'HTTPS://119.101.113.25:9999',
    'HTTPS://119.101.117.143:9999',
    'HTTPS://114.116.10.21:3128',
    'HTTPS://60.6.241.72:808',
    'HTTPS://113.105.170.139:3128',
    'HTTPS://114.99.2.201:9999',
    'HTTPS://119.101.113.213:9999',
]


def get_house_info(url):
    wb_data = requests.get(url, headers={'User-Agent': str(ua.random)})
    #  wb_data = requests.get(url, headers={'User-Agent': str(ua.random)},proxies={'https': random.choice(proxy)})
    if wb_data.status_code == 200:
        # 实测不用睡眠也可以实现抓取
        # time.sleep(1)  # 睡眠1秒
        soup = BeautifulSoup(wb_data.text, 'lxml')
        selector = Selector(text=wb_data.text)
        title = soup.select('h1')[0].get_text()  # 标题
        deal_date = soup.select('body > div.house-title > div > span')[0].get_text()  # 成交时间
        house_position = soup.select('div.myAgent > div.name > a')[0].get_text()  # 所处区域
        dealTotalPrice = soup.select('div.price > span > i')[0].get_text()   # 成交价格
        unit_price = soup.select('div.price > b')[0].get_text()  # 单价
        list_price = soup.select('div.info.fr > div.msg > span:nth-of-type(1) > label')[0].get_text()  # 挂牌价
        focus_num = soup.select('div.info.fr > div.msg > span:nth-of-type(5) > label')[0].get_text()  # 关注人数
        # floor = soup.select('div.base > div.content > ul > li:nth-of-type(2)')
        # 使用css selector会将li标签下的所有文字全都抓取过来，此处使用xpath
        floor = selector.xpath('//div[@class="base"]/div[2]/ul/li[2]/text()').extract()   # 楼层
        house_orientation = selector.xpath('//div[@class="base"]/div[2]/ul/li[7]/text()').extract()   # 房屋朝向
        built_date = selector.xpath('//div[@class="base"]/div[2]/ul/li[8]/text()').extract()  # 房屋建成年限
        elevator = selector.xpath('//div[@class="base"]/div[2]/ul/li[14]/text()').extract()  # 有无电梯
        subway = '有' if soup.find_all('a', 'tag is_near_subway') else '无'
        data = {
            'house_type': title.split(' ')[1],   # 户型
            'area': title.split(' ')[-1].split('平')[0],   # 房屋面积
            'deal_date': deal_date.split(' ')[0],   # 成交日期
            'house_position': house_position,   # 所属区域
            'dealTotalPrice': dealTotalPrice,   # 成交价格
            'unit_price': unit_price,  # 单价
            'list_price': list_price,  # 挂牌价
            'focus_num': focus_num,   # 关注人数
            'floor': floor[0].split('(')[0],  # 楼层数
            'house_orientation': house_orientation[0].split(' ')[0],   # 房屋朝向
            'built_date': built_date[0],   # 建成年限
            'elevator': elevator[0].split(' ')[0],   # 有无电梯
            'subway': subway  # 有无地铁
        }
        house_info_collection.insert_one(data)
        house_url_collection_success.insert_one({'house_id': url})   # 收集已抓取的url，ru

    else:
        missing_house_info.insert_one({'missing_house_url': url})  # 将抓取错误的url收集起来，如果出现错误就可以根据url重新抓取


if __name__ == '__main__':
    pool = Pool(processes=8)
    pool.map(get_house_info, list(data['house_url']))  # 使用pool的map函数
    pool.close()  # 关闭进程池，不再接受新的进程
    pool.join()  # 主进程阻塞等待子进程的退出







