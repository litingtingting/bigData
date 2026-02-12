# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase
from cfg import cfg
import time
from datetime import date, datetime
from pymongo import MongoClient,ReadPreference,DESCENDING
from bson.objectid import ObjectId

class cashDataMove(newsBase):

	_actType = {}

	def __init__(self):
		self._actType[1] = 'aaa'

	def _mapMove(cursor, db, collectName):
		data = []
		i = 0 
		for doc in cursor:
			i = i+1
			data.append(doc)
			if len(data) == 1000:
				print ('line:', i, ',write:', len(data))
				db[collectName].insert_many(data, ordered=False)
				data = []
				time.sleep(0.01)

		if len(data) >= 1:
			print ('line:', i, ',write:',len(data))
			db[collectName].insert_many(data, ordered=False)


	def move(data):
		printf('Cashmove')
		dbName = 'cash_seed'
		collectPrefix = 'user_cash_log_'
		todayTime = int(time.time())

		host = cfg.mongodb.get('cashseed.host')
		sourceClient = MongoClient(host)
		db_read = sourceClient.get_database(dbName, read_preference=ReadPreference.SECONDARY)
		#collection = db.user_cash_log

		host = cfg.mongodb.get('cashseed.bak_host')
		distClient = MongoClient(host)
		distdb = distClient[dbName]

		for i in range(100):
			query = {'time': {'$lt': todayTime-24*3600}}
			collectName = '{}{}'.format(collectPrefix, i)
			collection = db_read[collectName]

			for doc in distdb[collectName].find({} ,limit=1, sort=[('_id',DESCENDING)]):
				query['_id'] = {'$gt': doc['_id']}
				break;
			print('collection:',collectName,',query:',query)

			cursor = collection.find(query, projection={'ISODate':0})
			cashDataMove._mapMove(cursor, distdb, collectName)
			print ('delete-----')
			result = sourceClient[dbName][collectName].delete_many({'time': {'$lt': todayTime-91*24*3600}})
			print ('delete:', result.deleted_count)
			time.sleep(1)

		printf('Cashmove','end')


	def reback(data):
		dbName = 'cash_seed'
		collectionName = 'user_cash_log_0'

		host = cfg.mongodb.get('cashseed.host')
		sourceClient = MongoClient(host)
		db = sourceClient[dbName]

		host = cfg.mongodb.get('cashseed.bak_host')
		distClient = MongoClient(host)
		distdb = distClient[dbName]


		query = {'time': {'$lt': 1541319693}, '_id': {'$gt': ObjectId('5bd82b843fbbd058a3bb6019')}}

		cursor = distdb[collectionName].find(query)
		cashDataMove._mapMove(cursor, db,  collectionName)


	def goldstat(data):
		act_type = dict(cfg._conf.items('act_type'))
		dbName = 'goldbean'
		host = cfg.mongodb.get('cashseed.host')
		sourceClient = MongoClient(host)
		db_read = sourceClient.get_database(dbName, read_preference=ReadPreference.SECONDARY)

		host = cfg.mongodb.get('host')
		distClient = MongoClient(host)
		distdb = distClient['stats']

		today = str(date.today())
		todayTime = int(time.time())
		query = {'date': {'$eq': today}}

		collection = db_read['stat']
		data = {}
		for doc in collection.find(query):
			field = '{}_{}'.format(doc['act_type'],doc['source'])
			data[field] = {
				"seed_num" : doc['seed_num'], 
				"count" : doc['count'], 
				"user_count" : doc['user_count'],
				'title' : act_type.get(str(doc['act_type']),'默认标题')
			}
		data['update_time'] = todayTime
		data['date'] = today
		distdb['goldbean_stats'].update_one({'_id':today}, {'$set': data}, upsert=True)

	def cashstat(data):
		act_type = dict(cfg._conf.items('act_type'))
		dbName = 'cash'
		host = cfg.mongodb.get('cashseed.host')
		sourceClient = MongoClient(host)
		db_read = sourceClient.get_database(dbName, read_preference=ReadPreference.SECONDARY)

		host = cfg.mongodb.get('host')
		distClient = MongoClient(host)
		distdb = distClient['stats']

		today = str(date.today())
		todayTime = int(time.time())
		query = {'date': {'$eq': today}}

		collection = db_read['stat']
		data = {}
		for doc in collection.find(query):
			field = '{}_{}'.format(doc['act_type'],doc['source'])
			data[field] = {
				"seed_num" : round(doc['seed_num'],2), 
				"count" : doc['count'], 
				"user_count" : doc['user_count'],
				'title' : act_type.get(str(doc['act_type']),'默认标题')
			}
		data['update_time'] = todayTime
		data['date'] = today
		distdb['cash_stats'].update_one({'_id':today}, {'$set': data}, upsert=True)










	





		
