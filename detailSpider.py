import time

import gevent
import grequests
import pymongo
import requests
import tqdm
from lxml import etree

from tool import get_ip1, update_result, get_rawData, write_error


def detail_download(url, type, data=None):
    flag = True
    res = None
    while (flag):
        try:
            if type == 1:
                res = requests.post(url, data=data,timeout=100)
            else:
                res = requests.get(url)
            print(res)
        except:
            flag = True
            print('\n重新访问,detail')
            return res
        else:
            if res.status_code == 200:
                flag = False
            else:
                flag = True
    return res


def parse(res, place):
    data = {}
    data['url'] = res.url
    tree = etree.HTML(res.text)
    lib_code = tree.xpath('//div[@class="fl spdiv2"]')[0].xpath('string(.)').strip()
    bar_code = tree.xpath('//div[@class="fl spdiv2"]')[1].xpath('string(.)').strip()
    province = '四川省'
    country = place
    collector = tree.xpath('string(//div[@id="o_spcollter"]/.)')
    col_date = tree.xpath('string(//div[@id="o_spcoldate"]/.)')
    col_place = tree.xpath('string(//div[@id="o_spplace"]/.)')
    attitude = tree.xpath('string(//div[@id="o_spal"]/.)')
    images = 'http://img.cvh.ac.cn/imgcvh/l' + data['url'][21:] + '.jpg'
    sci_name = tree.xpath('string(//span[@id="splatin"]/.)')
    cn_name = tree.xpath('string(//div[@id="spcname"]/.)')
    data['lib_code'] = lib_code
    data['bar_code'] = bar_code
    data["_id"] = lib_code + ' ' + bar_code
    data['sci_name'] = sci_name
    data['cn_name'] = ''.join(cn_name).strip()
    data['Garde'] = " "
    data['reserve_name'] = " "
    data['Provice'] = province
    data['Country'] = country
    data['Collector'] = collector
    data['col_place'] = col_place
    data['col_date'] = col_date
    data['Altitude'] = ''.join(filter(str.isdigit, attitude))
    data['Image'] = images
    return data


def spider_main(client,data, place):
    res = detail_download('http://www.cvh.ac.cn/' + data['url'], type=0)
    print(res.url)
    global num, count
    num = num + 1
    print(place,num,'/',count)
    result = parse(res, place)
    result = get_character(res, result)
    update_result(client,result)


def detail_spider(client,place,v):
    global num, count
    num = v
    print('爬取详情数据',place)
    data = get_rawData(client,place)
    count=data.count()
    data=list(data[v:])
    for i in tqdm.tqdm(data,desc=place):
        spider_main(client,i, place)


def get_character(res, data):
    text = res.text
    spid = text[text.find('spid'):text.find('lo()')].strip().strip(';')
    spid = (''.join(filter(str.isdigit, spid)))
    info_res = detail_download('http://www.cvh.ac.cn/admin/getspinfo.ashx', type=1, data={'spid': spid, 's': '1'})
    try:
        tree = etree.HTML(info_res.json()['classtxt'])
    except Exception as e:
        print('\n重新访问,character',e)
    else:
        family = tree.xpath('//div')[-3].xpath('string(.)')
        genus = tree.xpath('//div')[-2].xpath('string(.)')
        cn_family = family.split()[0]
        family = family.split()[1]
        cn_genus = genus.split()[0]
        genus = genus.split()[1]
        data['Family'] = family
        data['cn_family'] = cn_family
        data['Genus'] = genus
        data['cn_genus'] = cn_genus
    finally:
        return data


if __name__ == '__main__':
    HOST = "127.0.0.1"
    PORT = 27017
    client = pymongo.MongoClient(HOST, PORT)#雅江县 3680     道孚县 3762  稻城县 4288 德昌县 2502 金阳县 1717 冕宁县 1883  美姑县 3610 盐源县 1952 木里藏族自治县 6613  道孚县 4475
    detail_spider(client,'稻城县',4392)# 德昌县 887 马尔康县 4329  泸定县  6350  康定县  6109  冕宁县 306
    write_error(client,'稻城县', 2)#都江堰市   5130   洪雅县   6247   屏山县   3290
    # HOST = "123.207.42.164"
    # PORT = 27017
    # client = pymongo.MongoClient(HOST, PORT)
    # order = {'德格县': 1721,  '雅江县': 1717, '米易县': 5076, '万源市': 1317, '汶川县': 1590,'南江县':458,'马边彝族自治县':1661,'洪雅县':1940}
    # for k,v in order.items():
    #     detail_spider(k,v)
    #     write_error(k,2)