# -*- coding: utf-8 -*-
# 组团任务统计


from common.funcs  import funcs, printf, mongoStats
from cfg import cfg
from pymongo import MongoClient,ReadPreference
from datetime import date
import time

class group(object):
	_today = str(date.today())
	_todayTime = funcs.date2time(_today)
	_nowTime = int(time.time())

	def pu(self,data):
		printf('Group.pu')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_group_pu',
			'statsCol': 'xz_group_pu_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','type':'$type'}}},
			{'$group':{'_id':{'type':'$_id.type'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
			if doc['_id']['type'] is None:
				doc['_id']['type'] = 0
			field = '{}_uv'.format(int(doc['_id']['type']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# PV
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'type':'$type'},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
			if doc['_id']['type'] is None:
				doc['_id']['type'] = 0
			field = '{}_pv'.format(int(doc['_id']['type']))
			update =  {'$set': {field:doc['pv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('Group.pu', 'end')

	def task(self,data):
		printf('Group.task')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_group_task',
			'statsCol': 'xz_group_task_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','type':'$type'}}},
			{'$group':{'_id':{'type':'$_id.type'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
			if doc['_id']['type'] is None:
				doc['_id']['type'] = 0
			field = '{}_uv'.format(int(doc['_id']['type']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 拉新未完成的人数
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client['stats']
		c = 'xz_group_task_date'
		collection = db[c]
		data = collection.find_one({'date':self._today})
		if data:
			print(data)
			# 发起邀请总人数
			totalNum = data.get('1_uv',0)
			# 完成人数
			groupNum = data.get('3_uv',0)
			ungroupNum = totalNum - groupNum

			# 拉新总人数
			inviteTotalNum = data.get('5_uv',0)
			# 拉满2人的人数
			invite2Num = data.get('2_uv',0)
			invite1Num = inviteTotalNum - invite2Num
			collection.update_one({'_id':data['_id']}, {'$set': {'4_uv':ungroupNum, '6_uv':invite1Num, 'date': self._today}},upsert=True)

		printf('Group.task', 'end')

Group = group()