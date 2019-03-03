def repair(item):
    # 复制第一条id相同的数据
    repeating = collection.find_one({'url': item})
    # 删除所有id相同的数据
    result = collection.delete_many({'url': item})
    # 把刚刚复制的数据加入一条到数据库
    col.insert_one(repeating)
    print(repeating)


def delete():
    global collection
    global col
    col=collection.city_4
    collection = collection.city_4
    data = collection.distinct('url')
    client.close()
    p = Pool(10)
    p.map(repair, list(data))
    p.close()
    p.join()