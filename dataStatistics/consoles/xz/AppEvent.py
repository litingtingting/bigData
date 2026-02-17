# -*- coding: utf-8 -*-
# app各种事件上报统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date


class appevent(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('AppEvent')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_app_event',
			'statsCol': 'xz_app_event_date',
		}

		# 事件UV
		pipeline = [
			{'$match': {'date': appevent._today}},
			{'$group':{'_id':{'d':'$d','event':'$event'}}},
			{'$group':{'_id':{'event':'$_id.event'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=appevent._today)
			field = '{}_uv'.format(doc['_id']['event'])
			update =  {'$set': {field:doc['uv'], 'date': appevent._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 事件PV
		pipeline = [
			{'$match': {'date': appevent._today}},
			{'$group':{'_id':{'event':'$event'},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=appevent._today)
			field = '{}_pv'.format(doc['_id']['event'])
			update =  {'$set': {field:doc['pv'], 'date': appevent._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('AppEvent', 'end')

AppEvent = appevent()