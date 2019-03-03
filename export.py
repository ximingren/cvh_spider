import os
from pprint import pprint

import pandas as pd
import pymongo


def sort():
    data = pd.read_csv('大邑县.csv')
    order = ['id', 'lib_code', 'bar_code', 'sci_name', 'cn_name', 'Family', 'cn_family', 'Genus', 'cn_genus', 'Grade'
        , 'reserve_name', 'Province', 'Country', 'Collector', 'col_place', 'col_date', 'Altitude', 'Image']
    data = data[order]
    data.to_excel('大邑县.xlsx', index=False)


def save_data(table, place):
    order = ['id', 'lib_code', 'bar_code', 'sci_name', 'cn_name', 'Family', 'cn_family', 'Genus', 'cn_genus', 'Grade',
             'reserve_name', 'Province', 'Country', 'Collector', 'col_place', 'col_date', 'Altitude', 'Image']
    try:
        temp = {}
        for i in table:
            for k, v in i.items():
                temp.setdefault(k, []).append(v)
        data = pd.DataFrame.from_dict(temp)
        data = data[order]
        data.to_excel('./data/' + place + '.xlsx', index=False)
    except Exception as e:
        print('保存数据错误', e)
    else:
        print('保存  %s 数据成功' % (place))


def get_data(place):
    HOST = "123.207.42.164"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT)
    collection = client.sichuan
    document = collection.city_4_result.find({'Country': place})
    total_data = []
    num = 0
    for i in document:
        num = num + 1
        i['id'] = num
        i['Grade']=i['Garde']
        i['Province']=i['Provice']
        del i['_id']
        del i['url']
        del i['Garde']
        del i['Provice']
        if (len(i) == 14):
            i['cn_genus'] = ""
            i['Family'] = ""
            i['Genus'] = ""
            i['cn_family'] = ""
        total_data.append(i)
    save_data(total_data, place)


if __name__ == '__main__':
    HOST = "123.207.42.164"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT)
    collection = client.sichuan
    document = collection.error_1.find({'type':2})
    for i in document:
        get_data(i['name'])
