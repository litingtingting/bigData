# -*- coding: utf-8 -*-

from cfg import cfg
from pymongo import MongoClient
import time
from datetime import date, timedelta
import json
from common.funcs  import funcs

# # 用户每次开宝箱操作
# db:source
# collection:mc_bh_openbox
# data:
# {
# 	"_id": ObjectId("5b504b7cfd4f0f508547dcc6"), 
# 	"seed_num": 10, //发放种子数量
# 	"open_times": 1, //每天第几次开启宝箱
# 	"ISOdate": ISODate("2018-07-19T08:27:40.114Z"),
# 	"date": "2018-07-19",
# 	"time": NumberLong("1531988860114"),
# 	"accum": 7, //连续几天开宝箱
# 	"tvmid": "123111"
# }

def printf(flag, ifbegin='begin', msg=''):
	tpl = '{date}:{msg} ==={flag} {begin}'

	if isinstance(msg, (list, dict)):
		msg = json.dumps(msg)

	tpl = tpl.format(flag=flag,
		date=time.strftime('%Y-%m-%d %H:%M:%S'),
		msg=msg,
		begin=ifbegin)
	print(tpl)


class openBox(object):
	"""用户分享数据统计"""
	def __init__(self, arg):
		super(openBox, self).__init__()
		self.arg = arg
		
	def continueOpenBox(self):
		printf('continueOpenBox')
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.source
		collection = db.mc_bh_openbox
		
		currentTime = int(time.time())
		# get the prev minite timestamp
		today = str(date.today())
		todayTime = funcs.date2time(today)
		updatetime = time.strftime('%Y-%m-%d %H:%M', time.localtime(currentTime))
		print('miniteTime:', updatetime)

		match = {'$match': {'time': {'$gte': todayTime*1000},
				'open_times': 1}}
		group = {'$group': {'_id': '$accum', 'total': {'$sum': 1}}}

		pipeline = []
		pipeline.append(match)
		pipeline.append(group)

		print(pipeline)

		cursor = collection.aggregate(pipeline)

		statsConnect = client.stats.mc_continue_openbox_date
		
		incUpdate = {
				'date': str(today),
				'update_time': updatetime,
		}
		for doc in cursor:
			stat_type = 'openbox_signed_{}'.format(int(doc['_id']))
			incUpdate[stat_type] = doc['total']

		if not incUpdate:
			print('empty')
			return

		update = {
			'$set': incUpdate
		}

		statsConnect.update_one({'_id': str(today)}, update, upsert=True)
		client.close()

		printf('continueOpenBox', 'over')


	def uvAward(self):
		printf('uvAward')
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.source
		collection = db.mc_bh_openbox

		today = str(date.today())
		todayTime = funcs.date2time(today)

		#uv
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}}})
		pipeline.append({'$group': {'_id': '$tvmid'}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})
		print(pipeline)
		cursor = collection.aggregate(pipeline)

		for doc in cursor:
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set': {'openbox_uv': doc['total']}}, upsert=True)

		#award
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': '$seed_num'}}})
		print(pipeline)
		cursor = collection.aggregate(pipeline)
		for doc in cursor:
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set':{'openbox_award': doc['total'], 'date': str(today)}}, upsert=True)
		cursor.close()
		printf('uvAward', 'end')

	def newUvAward(self):
		printf('newUvAward')
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.source
		collection = db.mc_bh_openbox

		today = str(date.today())
		todayTime = funcs.date2time(today)

		#uv
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}, 'is_new': 1}})
		pipeline.append({'$group': {'_id': '$tvmid'}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})

		print(pipeline)
		for doc in collection.aggregate(pipeline):
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set': {'new_openbox_uv': doc['total']}}, upsert=True)

		#award
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}, 'is_new': 1}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': '$seed_num'}}})
		print(pipeline)
		cursor = collection.aggregate(pipeline)
		for doc in cursor:
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set':{'new_openbox_award': doc['total'], 'date': str(today)}}, upsert=True)
		cursor.close()
		printf('newUvAward', 'end')


	def mCenterPageUv(self):
		printf('mCenterPageUv')

		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior
		collection = db.mc_page_load

		today = str(date.today())
		todayTime = funcs.date2time(today)

		query = {'cu': '/cdn/html/moneyCenter/index.html',
				'time': {'$gte': todayTime*1000}}
		res = collection.find(query).count()
		print('pv:', res)

		statCollect = client.stats.mc_openbox_date

		if res: 
			statCollect.update_one({'_id': str(today)},
				{'$set': {'mcenter_page_pv': res}},
				upsert=True)
		#uv
		pipeline = []
		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': '$d'}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})
		print(pipeline)

		for doc in collection.aggregate(pipeline):
			statCollect.update_one({'_id': str(today)},
				{'$set': {'mcenter_page_uv': doc['total'], 'date': str(today)}},
				upsert=True)
		printf('mCenterPageUv', 'end')


	def newShareBox(self):
		printf('newShareBox')

		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.source
		collection = db.mc_nu_sb_st

		today = str(date.today())
		todayTime = funcs.date2time(today)

		#uv
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}}})
		pipeline.append({'$group': {'_id': '$tvmid'}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})
		print (pipeline)

		for doc in collection.aggregate(pipeline):
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set': {'new_sharebox_uv': doc['total']}}, upsert=True)

		#award
		pipeline = []
		pipeline.append({'$match': {'time': {'$gte': todayTime*1000}}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': '$seed_num'}}})
		for doc in collection.aggregate(pipeline):
			print(doc)
			client.stats.mc_openbox_date.update_one({'_id': str(today)}, {'$set':{'new_sharebox_award': doc['total']}}, upsert=True)

		printf('newShareBox', 'end')


	def reloadBoxData(self):
		printf('reloadBoxData')

		host = cfg.mongodb.get('cashseed.host')
		client = MongoClient(host)
		db = client['cash_seed']
		collection = db['stat']

		client1 = MongoClient(cfg.mongodb.get('host'))
		db_stat = client1['stats']
		stat_collection = db_stat['mc_openbox_date']

		today = date.today()
		
		query = {'date': str(today), 'act_type': {'$in': [29, 15]}}

		for doc in collection.find(query):
			if doc['act_type'] == 15:
				field = 'sharebox'
			elif doc['act_type'] == 29:
				field = 'paytribute'

			updateOne = {field+'_uv': doc['user_count'], field+'_award': doc['seed_num']}
			print (updateOne)
			stat_collection.update_one({'_id': str(today)}, {'$set': updateOne}, upsert=True)

		printf('reloadDataShare', 'end')

		








