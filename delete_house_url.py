import pymongo
import pandas as pd


cilent = pymongo.MongoClient('localhost', 27017)
lianjia = cilent['lianjia']
house_url_collection = lianjia['house_url']
house_url_collection_succcess = lianjia['house_url_collection_success']


data = pd.DataFrame(list(house_url_collection_succcess.find()))
data1 = pd.DataFrame(list(house_url_collection.find()))
for url in list(data['house_id']):
    if url in list(data1['house_url']):
        house_url_collection.delete_one({'house_url': url})
    else:
        pass