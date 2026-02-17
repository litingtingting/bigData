# -*- coding: utf-8 -*-
# 签到统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date


class checkin(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('Checkin')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_checkin',
			'statsCol': 'xz_checkin_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': checkin._today}},
			{'$group':{'_id':{'d':'$d'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=checkin._today)
			update =  {'$set': {'uv':doc['uv'], 'date': checkin._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 新用户UV
		pipeline = [
			{'$match': {'date': checkin._today,'is_new':1}},
			{'$group':{'_id':{'d':'$d'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=checkin._today)
			update =  {'$set': {'new_uv':doc['uv'], 'date': checkin._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 发放量
		pipeline = [
			{'$match': {'date': checkin._today}},
			{'$group':{'_id':'','total':{'$sum':'$reward'}}},
			# {'$group':{'_id':{'type':'$_id.type'},'total':{'$sum':'$num'}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=checkin._today)
			update =  {'$set': {'total':doc['total'], 'date': checkin._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 连续几天签到
		pipeline = [
			{'$match': {'date': checkin._today}},
			{'$group':{'_id':{'d':'$d','continuous_day':'$continuous_day'}}},
			{'$group':{'_id':{'continuous_day':'$_id.continuous_day'},'total':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=checkin._today)
			field = '{}_continuous_uv'.format(int(doc['_id']['continuous_day']))
			update =  {'$set': {field:doc['total'], 'date': checkin._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('Checkin', 'end')

Checkin = checkin()