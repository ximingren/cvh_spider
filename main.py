import os
import threading
from functools import partial

import pandas as pd
import re
from multiprocessing.pool import Pool
import pymongo
import requests

from detailSpider import detail_spider
from listSpider import list_spider
from tool import write_error, insert_one, update_result


def get_name():
    global collection
    data = collection.error_1.find({'type': 2})
    raw_data = collection.error_1.find({'type': 3})
    no_data = collection.error_1.find({'type': 0})
    name_list = []
    for i in data:
        name_list.append(i['name'])
    for i in raw_data:
        name_list.append(i['name'])
    for i in no_data:
        name_list.append(i['name'])
    return name_list


def get_count(place):
    global collection
    count = collection.city.find({'Country': place}).count()
    return count


def get_number(place):
    text = '时间已到'
    result = None
    while ('时间已到' in text):
        try:
            res = requests.get('http://www.cvh.ac.cn/search/%s?page=%s&n=4' % (place, 1))
        except:
            text = '时间已到'
        else:
            text = res.text
            result = ''.join(re.findall('共\d+号', text))
    return ''.join(filter(str.isdigit, result))


def main(i):
    global client, collection
    client = pymongo.MongoClient(HOST, PORT)
    collection = client.sichuan
    if i in ['北川县', '攀枝花市西区', '彭州市', '叙永县', '旺苍县', '安县', '北川羌族自治县', '马边彝族自治县', '白玉县',
             '峨边彝族自治县', '荣县', '大邑县', '崇州市', '剑阁县', '邻水县', '泸县', '射洪县', '沐川县', '阆中市', '宣汉县', '蓬安县', '苍溪县', '彭州市', '合江县',
             '长宁县']:
        write_error(client, i, 3)
    if i not in get_name():
        num = get_number(i)
        count = get_count(i)
        if int(count) < int(num):
            # list_spider(client, i, int(int(count) / 15), int(int(num) / 15))
            # thread=threading.Thread(target=list_spider,args=(client,i, int(int(count) / 15), int(int(num) / 15)))
            # thread.setDaemon(True)
            # thread.start()
            list_spider(client, i, int(int(count) / 15), int(int(num) / 15))
            write_error(client, i, 0)
            # thread = threading.Thread(target=detail_spider, args=(client, i, 0))
            # thread.setDaemon(True)
            # thread.start()
            # detail_spider(client, i, 0)
            # write_error(client, i, 2)
        elif int(num) == 0:
            write_error(client, i, 3)
        # else:
        #     detail_spider(client, i, 0)
        #     write_error(client, i, 2)
            # thread = threading.Thread(target=detail_spider, args=(client, i, 0))
            # thread.setDaemon(True)
            # thread.start()
            # write_error(client, i, 2)
    client.close()





if __name__ == '__main__':
    HOST = "127.0.0.1"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT)
    collection = client.sichuan
    data = pd.read_excel('需要的县.xlsx')
    p = Pool()
    p.map(main, data.iloc[:, 0])
    p.close()
    p.join()

