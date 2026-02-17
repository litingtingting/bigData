# -*- coding: utf-8 -*-
# 注册人数统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date

class uvpv(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('uvpv')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_common_pvuv',
			'statsCol': 'xz_common_pvuv_date',
		}

		# UV
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','t':'$t'}}},
			{'$group':{'_id':{'t':'$_id.t'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = '{}_uv'.format(doc['_id']['t'])
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# PV
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'t':'$t'},'pv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = '{}_pv'.format(doc['_id']['t'])
			update =  {'$set': {field:doc['pv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('uvpv', 'end')

Uvpv = uvpv()