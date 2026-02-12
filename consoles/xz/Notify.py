# -*- coding: utf-8 -*-
# 消息统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date


class notify(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('Notify')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_app_notify',
			'statsCol': 'xz_app_notify_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': notify._today}},
			{'$group':{'_id':{'d':'$d','event':'$event','type':'$type'}}},
			{'$group':{'_id':{'event':'$_id.event','type':'$_id.type'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=notify._today)
			field = '{}.{}_uv'.format(doc['_id']['type'],doc['_id']['event'])
			update =  {'$set': {field:doc['uv'], 'date': notify._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# PV
		pipeline = [
			{'$match': {'date': notify._today}},
			{'$group':{'_id':{'event':'$event','type':'$type'},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=notify._today)
			field = '{}.{}_pv'.format(doc['_id']['type'],doc['_id']['event'])
			update =  {'$set': {field:doc['pv'], 'date': notify._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('Notify', 'end')

Notify = notify()