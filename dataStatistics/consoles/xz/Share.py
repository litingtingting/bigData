# -*- coding: utf-8 -*-
# 分享统计

from common.funcs  import funcs, printf, mongoStats
from cfg import cfg
from pymongo import MongoClient,ReadPreference
from datetime import date

class share(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('share.Stat')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_share',
			'statsCol': 'xz_share_date',
		}

		# 分享次数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'channel':'$channel','type':'$type'},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = '{}.{}_pv'.format(doc['_id']['channel'],doc['_id']['type'])
			update =  {'$set': {field:doc['pv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 分享人数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','channel':'$channel','type':'$type'}}},
			{'$group':{'_id':{'channel':'$_id.channel','type':'$_id.type'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = '{}.{}_uv'.format(doc['_id']['channel'],doc['_id']['type'])
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 注册当日发起分享人数
		pipeline = [
			{'$match': {'date': self._today,'is_today':1}},
			{'$group':{'_id':{'d':'$d','channel':'$channel','type':'$type'}}},
			{'$group':{'_id':{'channel':'$_id.channel','type':'$_id.type'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = '{}.{}_regisday_uv'.format(doc['_id']['channel'],doc['_id']['type'])
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)


		printf('share.Stat','end')


	def newUv(self,data):
		print("share.newUv")

		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.get_database('user_behavior', read_preference=ReadPreference.SECONDARY)
		c = 'xz_share'
		collection = db[c]

		pipeline = [
			{'$match': {'date': self._today,'type':'share'}},
			{'$group':{'_id':{'d':'$d','channel':'$channel'}}},
		]

		data = {}
		for doc in collection.aggregate(pipeline).batch_size(5):
			count = collection.find({'d':doc['_id']['d'],'channel':doc['_id']['channel'],'date':{'$ne':self._today}}).count()
			if count <= 0:
				field = '{}.new_share_uv'.format(doc['_id']['channel'])
				if not data.get(field):
					data[field] = 0
				data[field] += 1

		
		statDb = client['stats']
		_id = '{}_{}'.format(c,self._today)
		data['date'] = self._today
		print(data)

		update = {'$set' : data}
		statCol = '{}_date'.format(c)
		statDb[statCol].update_one({'_id':_id}, update,upsert=True)

		print("share.newUv","end")


Share = share()