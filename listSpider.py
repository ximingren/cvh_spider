import pymongo
import requests
from lxml import etree
from tqdm import trange

from test_case.cvh_spider.tool import insert_one, write_error, get_ip1


def list_download(place, page):
    text = '时间已到'
    res = None
    while ('时间已到' in text):
        try:
            res = requests.get('http://www.cvh.ac.cn/search/%s?page=%s&n=4' % (place, page), proxies=get_ip1(),
                               timeout=2000)
        except:
            text = '时间已到'
            print('重新访问,list',place)
        else:
            text = res.text

    return res

def list_spider(client,place, start=1, pages=None):
    print('开始爬取', place)
    try:
        for page in trange(start, int(pages) + 1):
            res = list_download(place, page)
            tree = etree.HTML(res.text)
            tr_list = tree.xpath('//tr')
            for i in tr_list[1:]:
                if i.xpath('string(@class)') == 'bc1':
                    data = {}
                    code=i.xpath('./td')[0].xpath('string(.)')
                    sci_name = i.xpath('./td')[1].xpath('string(.)')
                    cn_name = i.xpath('./td')[2].xpath('string(.)')
                    url = i.xpath('./td')[-1].xpath('./a/@href')[0]
                    data['code'] = code
                    data['sci_name'] = sci_name
                    data['cn_name'] = cn_name
                    data['url'] = url
                    data['Country'] = place
                    insert_one(client,data)
        write_error(client,place, 0)
    except Exception as e:
        print(e)
        write_error(client,place, 1)
