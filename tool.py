import json
import random

import requests


def get_ip1():
    # scylla
    response = requests.get('http://localhost:8899/api/v1/proxies?anonymous=True&https=true')
    data = json.loads(response.text)
    proxies = random.choice(data['proxies'])
    if proxies['is_https']:
        proxy = {'https': 'https://' + str(proxies['ip']) + ":" + str(proxies['port'])}
    else:
        proxy = {'http': 'http://' + str(proxies['ip']) + ":" + str(proxies['port'])}
    return proxy


def get_rawData(client,place):
    collection = client.sichuan
    data = collection.city.find({'Country': place})
    client.close()
    return data


def update_result(client,result):
    collection = client.sichuan
    try:
        # collection.city_1.update({'url': result['url']}, {'$set': result})
        collection.city_result.insert(result)
        client.close()
    except Exception as e:
        print('更新数据失败', e, result)
    else:
        print('更新数据成功  ', result['sci_name'])



def insert_one(client,data):
    collection = client.sichuan
    try:
        collection.city.insert(data)
        client.close()
    except Exception as e:
        print('保存数据失败  ',data['Country']+'  ', e)
    else:
        print('保存数据成功  ', data['cn_name'] + '   ', data['Country'])


def write_error(client,name, type):
    collection = client.sichuan
    collection.error_1.update({'name': name}, {'$set': {'name': name, 'type': type}},upsert=True)
