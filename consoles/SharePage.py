# -*- coding: utf-8 -*-

from cfg import cfg
from pymongo import MongoClient
from bson.code import Code
import time
from datetime import date

class SharePage(object):
	"""用户分享数据统计"""
	def __init__(self, arg):
		super(sharePage, self).__init__()
		self.arg = arg
		
	def stat(self):
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior
		collection = db.sharepage
		today = date.today()
		todayTime = int(time.mktime(today.timetuple()))

		pipeline = []
		pipeline.append({'$match': {'date': str(today)}})
		pipeline.append({'$group': {'_id': '$type','count':{'$sum':1}}})
		list = collection.aggregate(pipeline)

		mapFunc = Code("function(){emit(this.tvmid,1)}")
		reduceFunc = Code("function(key,values){return Array.sum(values)}")
		for item in list:
			uv = collection.map_reduce(mapFunc, reduceFunc, "result",query={'type':item['_id'],'date':str(today)}).find().count()
			_id = '{}_{}'.format(item['_id'],todayTime)
			client.stats.sharepage.update_one({'_id':_id}, {'$set':{'pv':item['count'],'uv':uv,'date':str(today),'type':item['_id']}},upsert=True)
