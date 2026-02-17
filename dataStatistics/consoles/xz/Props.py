# -*- coding: utf-8 -*-
# 道具卡统计

from common.funcs  import funcs, printf, mongoStats
from cfg import cfg
from pymongo import MongoClient,ReadPreference
from datetime import date
import time

class props(object):
	_today = str(date.today())
	_todayTime = funcs.date2time(_today)
	_nowTime = int(time.time())

	def Stat(self,data):
		host = cfg.mongodb.get('cashseed.host')
		client = MongoClient(host)
		db = client.get_database('prop_card', read_preference=ReadPreference.SECONDARY)
		collectionPrefix = 'user_prop'

		cardTypes = ['double_card','goldbean_card']
		list = {}
		# field['grant_num'] = 0 # 发放量
		# field['hold_num']= 0 # 持有量
		# field['using_num'] = 0 # 使用中的数量
		# field['used_num'] = 0 # 已使用完成的数量
		# field['used_new_num'] = 0 # 新用户已使用完成的数量
		# field['expired_num'] = 0 # 失效的数量
		# field['use_uv'] = 0 # 使用人数
		# field['used_uv'] = 0 # 使用完成人数
		# field['double_seed_num'] = 0 # 道具翻倍金豆发放量
		for i in range(0,99):
			collectionName = '{}_{}'.format(collectionPrefix,i)
			collection = db[collectionName]

			pipeline = [
				{'$match': {'draw_time' : {'$gt' : self._todayTime}}},
				{'$group':{'_id':{'card_type':'$card_type'},'num':{'$sum':1}}},
			]
			self.aggregateStats(collection, pipeline, list, 'grant_num')

			pipeline = [
				{'$match': {'start_time':0,'expire_time' : {'$gt' : self._nowTime}}},
				{'$group':{'_id':{'card_type':'$card_type'},'num':{'$sum':1}}},
			]
			self.aggregateStats(collection, pipeline, list, 'hold_num')

			selector = {
				'card_type': cardTypes[0],
				'start_time' : {'$gt' : 0},
				'available_times' : {'$gt' : 0},
				'expire_time' : {'$gt' : self._nowTime}
			}
			self.insertField(list, cardTypes[0], 'using_num', collection.find(selector).count())

			pipeline = [
				{'$match': {
					'last_use_time' : {'$gt' : self._todayTime},
					'available_times' : {'$lte' : 0}
				}},
				{'$group':{'_id':{'card_type':'$card_type'},'num':{'$sum':1}}},
			]

			self.aggregateStats(collection, pipeline, list, 'used_num')

			pipeline = [
				{'$match': {
					'last_use_time' : {'$gt' : self._todayTime},
					'available_times' : {'$lte' : 0},
					'draw_time' : {'$gt' : self._todayTime},
					'is_new' : 1
				}},
				{'$group':{'_id':{'card_type':'$card_type'},'num':{'$sum':1}}},
			]

			self.aggregateStats(collection, pipeline, list, 'used_new_num')

			pipeline = [
				{'$match': {
					'expire_time' : {'$gt' : self._todayTime,'$lt' : self._nowTime},
					'available_times' : {'$gt' : 0}
				}},
				{'$group':{'_id':{'card_type':'$card_type'},'num':{'$sum':1}}},
			]

			self.aggregateStats(collection, pipeline, list, 'expired_num')

			pipeline = [
				{'$match': {'card_type':cardTypes[0],'start_time' : {'$gt' : self._todayTime}}},
				{'$group':{'_id':{'tvmid':'$tvmid','card_type':'$card_type'}}},
				{'$group':{'_id':{'card_type':'$_id.card_type'},'num':{'$sum':1}}},
			]
			self.aggregateStats(collection, pipeline, list, 'use_uv')

			pipeline = [
				{'$match': {'last_use_time' : {'$gt' : self._todayTime},'available_times' : {'$lte' : 0}}},
				{'$group':{'_id':{'tvmid':'$tvmid','card_type':'$card_type'}}},
				{'$group':{'_id':{'card_type':'$_id.card_type'},'num':{'$sum':1}}},
			]
			self.aggregateStats(collection, pipeline, list, 'used_uv')

		statHost = cfg.mongodb.get('host')
		statClient = MongoClient(statHost)
		statDb = statClient['stats']
		_id = 'xz_props_{}'.format(self._today)
		list['date'] = self._today
		update = {'$set' : list}
		statDb['xz_props_date'].update_one({'_id':_id}, update,upsert=True)

	def Goldbean(self,data):
		""" 使用道具卡发放的金豆统计 """
		host = cfg.mongodb.get('cashseed.host')
		client = MongoClient(host)
		db = client.get_database('goldbean', read_preference=ReadPreference.SECONDARY)
		collectionPrefix = 'user_cash_log'

		cardTypes = ['double_card','goldbean_card']
		field = {}
		for i in range(0,99):
			collectionName = '{}_{}'.format(collectionPrefix,i)
			collection = db[collectionName]

			# 10014:阅读
			pipeline = [
				{'$match': {'card_id' : {'$ne' : ''},'time':{'$gt':self._todayTime}}},
				{'$group':{'_id':{'act_type':'$act_type'},'total':{'$sum':'$num'}}},
			]

			for doc in collection.aggregate(pipeline):
				f = '{}.{}_double_seed_num'.format(cardTypes[0],doc['_id']['act_type'])
				if doc['total'] > 0:
					if f in field:
						field[f] += doc['total']
					else:
						field[f] = doc['total']

		statHost = cfg.mongodb.get('host')
		statClient = MongoClient(statHost)
		statDb = statClient['stats']
		_id = 'xz_props_{}'.format(self._today)
		field['date'] = self._today
		update = {'$set' : field}
		statDb['xz_props_date'].update_one({'_id':_id}, update,upsert=True)

	def insertField(self,list,cardType,field,num):
		f = '{}.{}'.format(cardType, field)
		if not list.get(f):
			list[f] = num
		else:
			list[f] += num

	def aggregateStats(self,collection,pipeline,list,field):
		for doc in collection.aggregate(pipeline):
			card_type = doc['_id']['card_type']
			if not card_type:
				continue
			self.insertField(list, card_type, field, doc['num'])

Props = props()
