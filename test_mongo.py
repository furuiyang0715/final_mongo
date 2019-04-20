import pymongo

client = pymongo.MongoClient(host='172.19.0.1', port=27017)
db = client.datacenter
try:
    res = db.sync_info.find().count()
    print(res)
except Exception as e:
    print(e)
