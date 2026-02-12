# -*- coding: utf-8 -*-
# 师徒相关统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date

class st(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('st')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_st_apprentice',
			'statsCol': 'xz_st_apprentice_date',
		}

		# 新增好友人数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'sd':'$sd','source':'$source'}}},
			{'$group':{'_id':{'source':'$_id.source'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'sd_uv_{}'.format(int(doc['_id']['source']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 新增邀请人数
		pipeline = [
			{'$match': {'date': self._today,'is_new':1}},
			{'$group':{'_id':{'d':'$d','source':'$source'}}},
			{'$group':{'_id':{'source':'$_id.source'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'uv_{}'.format(int(doc['_id']['source']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('st', 'end')

St = st()