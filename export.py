import os
from multiprocessing.pool import Pool
from pprint import pprint

import pandas as pd
import pymongo

from detailSpider import detail_spider
from tool import write_error

real_dict = [
    {'place': '蒲江县', 'num': '121'}, {'place': '都江堰市', 'num': '12220'}, {'place': '彭州市', 'num': '218'},
    {'place': '崇州市', 'num': '584'}, {'place': '荣县', 'num': '74'}, {'place': '攀枝花市西区', 'num': '0'},
    {'place': '米易县', 'num': '5609'}, {'place': '盐边县', 'num': '2533'}, {'place': '泸州市纳溪区', 'num': '0'},
    {'place': '行政区域', 'num': '0'}, {'place': '泸县', 'num': '88'}, {'place': '合江县', 'num': '645'},
    {'place': '叙永县', 'num': '4930'}, {'place': '古蔺县', 'num': '1880'}, {'place': '广汉市', 'num': '338'},
    {'place': '什邡市', 'num': '5'}, {'place': '绵竹市', 'num': '1'}, {'place': '绵阳市游仙区', 'num': '0'},
    {'place': '三台县', 'num': '82'}, {'place': '盐亭县', 'num': '60'}, {'place': '安县', 'num': '812'},
    {'place': '北川羌族自治县', 'num': '0'}, {'place': '平武县', 'num': '10028'}, {'place': '江油市', 'num': '84'},
    {'place': '广元市朝天区', 'num': '0'}, {'place': '旺苍县', 'num': '837'}, {'place': '青川县', 'num': '538'},
    {'place': '梓剑阁县', 'num': '0'}, {'place': '元坝县', 'num': '0'}, {'place': '剑阁县', 'num': '598'},
    {'place': '苍溪县', 'num': '684'}, {'place': '射洪县', 'num': '14'}, {'place': '内江市东兴区', 'num': '0'},
    {'place': '资中县', 'num': '0'}, {'place': '乐山市五通桥区', 'num': '0'}, {'place': '乐山市金口河区', 'num': '0'},
    {'place': '沐川县', 'num': '162'}, {'place': '峨边彝族自治县', 'num': '4305'},
    {'place': '马边彝族自治县', 'num': '2519'}, {'place': '南充市嘉陵区', 'num': '0'}, {'place': '蓬安县', 'num': '9'},
    {'place': '阆中市', 'num': '55'}, {'place': '洪雅县', 'num': '8637'}, {'place': '宜宾县', 'num': '180'},
    {'place': '长宁县', 'num': '217'}, {'place': '筠连县', 'num': '1493'}, {'place': '屏山县', 'num': '4047'},
    {'place': '邻水县', 'num': '129'}, {'place': '达州市通川区', 'num': '0'}, {'place': '宣汉县', 'num': '107'},
    {'place': '万源市', 'num': '4156'}, {'place': '雅安市雨城区', 'num': '0'}, {'place': '雅安市雨城荥经县', 'num': '0'},
    {'place': '石棉县', 'num': '12730'},
    {'place': '天全县', 'num': '32603'},
    {'place': '宝兴县', 'num': '22761'},
    {'place': '通江县', 'num': '1148'}, {'place': '南江县', 'num': '1431'}, {'place': '平昌县', 'num': '67'},
    {'place': '安岳县', 'num': '88'}, {'place': '乐至县', 'num': '39'}, {'place': '简阳市', 'num': '125'},
    {'place': '汶川县', 'num': '9514'},
    {'place': '理县', 'num': '17616'},
    {'place': '茂县', 'num': '7485'},
    {'place': '松潘县', 'num': '8492'}, {'place': '九寨沟县', 'num': '2833'}, {'place': '若尔盖县', 'num': '5351'},
    {'place': '金川县', 'num': '6607'}, {'place': '小金县', 'num': '7852'}, {'place': '黑水县', 'num': '6481'},
    {'place': '马尔康县', 'num': '25525'}, {'place': '壤塘县', 'num': '2685'}, {'place': '阿坝县', 'num': '2360'},
    {'place': '红原县', 'num': '6156'},
    {'place': '康定县', 'num': '41951'},
    {'place': '泸定县', 'num': '24207'},
    {'place': '丹巴县', 'num': '1365'}, {'place': '九龙县', 'num': '8329'}, {'place': '雅江县', 'num': '4042'},
    {'place': '道孚县', 'num': '5764'}, {'place': '雅道孚县', 'num': '0'}, {'place': '炉霍县', 'num': '1232'},
    {'place': '甘孜县', 'num': '2606'}, {'place': '新龙县', 'num': '898'}, {'place': '德格县', 'num': '4465'},
    {'place': '白玉县', 'num': '937'}, {'place': '石渠县', 'num': '2323'}, {'place': '色达县', 'num': '2645'},
    {'place': '理塘县', 'num': '2151'}, {'place': '稻城县', 'num': '12925'}, {'place': '巴塘县', 'num': '3671'},
    {'place': '乡城县', 'num': '11054'}, {'place': '得荣县', 'num': '2217'}, {'place': '普格县', 'num': '1943'},
    {'place': '德昌县', 'num': '3141'}, {'place': '木里藏族自治县', 'num': '27109'}, {'place': '盐源县', 'num': '4872'},
    {'place': '布拖县', 'num': '1742'}, {'place': '金阳县', 'num': '3338'}, {'place': '冕宁县', 'num': '4573'},
    {'place': '越西县', 'num': '2612'}, {'place': '甘洛县', 'num': '1406'}, {'place': '美姑县', 'num': '4835'},
    {'place': '雷波', 'num': '147'}]


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
        data.to_excel('./data2/' + place + '.xlsx', index=False)
    except Exception as e:
        print('保存数据错误',place, e)
    else:
        print('保存  %s 数据成功' % (place))


def validate(i):
    # data_dir = '/home/ximingren/Projects/Projects/spider/cvh_spider/data/'
    # files = os.listdir(data_dir)
    # for i in files:
    #     print(i)
    #     data = pd.read_excel(data_dir + i)
    #     for v, k in data.iterrows():
    #         result = dict(k)
    #         if 'id' in result.keys():
    #             del result['id']
    #         update_result(client, dict(k))
    documents = collection.city_result.find({'Country': i['place']})
    count = documents.count()
    # if i['place']not in ['平昌县','乐至县','若尔盖县','金川县','小金县','泸定县','雅江县','道孚县','炉霍县','稻城县']:
    if int(count)!=0:
        get_data(i['place'])
    # print('big', i['place'], count, i['num'])
#         print(i['place'], count, i['num'])
#         detail_spider(client, i['place'], count)
#         write_error(client, i['place'], 2)
#     else:
#         write_error(client, i['place'], 2)


def get_data(place):
    HOST = "127.0.0.1"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT)
    collection = client.sichuan
    document = collection.city_result.find({'Country': place})
    total_data = []
    num = 0
    for i in document:
        num = num + 1
        i['id'] = num
        if 'Garde' in i.keys():
            i['Grade'] = i['Garde']
            del i['Garde']
        if 'Provice' in i.keys():
            i['Province'] = i['Provice']
            del i['Provice']
        del i['_id']
        if 'url' in i.keys():
            del i['url']
        if (len(i) == 14):
            i['cn_genus'] = ""
            i['Family'] = ""
            i['Genus'] = ""
            i['cn_family'] = ""
        total_data.append(i)
    save_data(total_data, place)


if __name__ == '__main__':
    HOST = "127.0.0.1"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT, connect=False)
    collection = client.sichuan
    # document = collection.error_1.find({'type': 2})
    # for i in document:
    #     get_data(i['name'])
    for i in real_dict:
        validate(i)
