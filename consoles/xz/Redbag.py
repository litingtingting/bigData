# -*- coding: utf-8 -*-
# 刷红包统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date


class redbag(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('Redbag')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_redbag',
			'statsCol': 'xz_redbag_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': redbag._today}},
			{'$group':{'_id':{'d':'$d'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=redbag._today)
			update =  {'$set': {'uv':doc['uv'], 'date': redbag._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# PV
		pipeline = [
			{'$match': {'date': redbag._today}},
			{'$group':{'_id':{},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=redbag._today)
			update =  {'$set': {'pv':doc['pv'], 'date': redbag._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 发放量
		pipeline = [
			{'$match': {'date': redbag._today}},
			{'$group':{'_id':'$type','total':{'$sum':'$num'}}},
			# {'$group':{'_id':{'type':'$_id.type'},'total':{'$sum':'$num'}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			_id = '{source}_{date}'.format(source=conf['queryCol'] ,date=redbag._today)
			field = '{}_total'.format(int(doc['_id']))
			update =  {'$set': {field:doc['total'], 'date': redbag._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('Redbag', 'end')

Redbag = redbag()