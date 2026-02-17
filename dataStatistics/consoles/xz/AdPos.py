# -*- coding: utf-8 -*-
# 广告资源位统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date
from pymongo import MongoClient,ReadPreference

class adpos(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('AdPos')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_ad_pos',
			'statsCol': 'xz_ad_pos_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': adpos._today}},
			{'$group':{'_id':{'d':'$d','event':'$event','tid':'$tid'}}},
			{'$group':{'_id':{'event':'$_id.event','tid':'$_id.tid'},'uv':{'$sum':1}}},
		]
		# print(pipeline)

		updateData = {}
		_id = '{source}_{date}'.format(source=conf['queryCol'],date=adpos._today)

		def mapFunc(doc,data):
			print(doc)
			event = doc['_id']['event']
			tid = int(doc['_id'].get('tid',0))
			if not updateData.get(event):
				updateData[event] = []

			idData = {}
			idData['tid'] = tid
			idData['uv'] = doc['uv']
			updateData[event].append(idData)

			return '','',conf['statsCol'],False

		def reduceFunc(client):
			if not updateData:
				return
			db_read = client.get_database('stats', read_preference=ReadPreference.SECONDARY)
			update = {'$set':updateData}
			db_read[conf['statsCol']].update_one({'_id':_id},update,upsert=True)
		mongoStats(conf,pipeline,mapFunc,reduceFunc)

		# PV
		pipeline = [
			{'$match': {'date': adpos._today}},
			{'$group':{'_id':{'event':'$event','tid':'$tid'},'pv':{'$sum':1}}},
		]
		print(pipeline)


		def mapFunc(doc,data):
			print(doc)
			tid = int(doc['_id'].get('tid',0))
			find = '{}.{}'.format(doc['_id']['event'],'tid')
			query = {'_id':_id,find:tid}
			field = '{}.$.{}'.format(doc['_id']['event'],'pv')
			
			update =  {'$set': {field:doc['pv'], 'date': adpos._today}}
			return query,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('AdPos', 'end')

AdPos = adpos()