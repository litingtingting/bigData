# -*- coding: utf-8 -*-
# 收徒批次奖励统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date

class batch(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('batch')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_st_batch',
			'statsCol': 'xz_st_batch_date',
		}

		# 贡献奖励的好友数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'sd':'$sd','batch':'$batch'}}},
			{'$group':{'_id':{'batch':'$_id.batch'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'sd_uv_{}'.format(int(doc['_id']['batch']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 奖励获得人数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','batch':'$batch'}}},
			{'$group':{'_id':{'batch':'$_id.batch'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'sf_uv_{}'.format(int(doc['_id']['batch']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 奖励发放总额
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'batch':'$batch'},'num':{'$sum':'$reward'}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'reward_{}'.format(int(doc['_id']['batch']))
			update =  {'$set': {field:doc['num'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('batch', 'end')

	# def top():
	# 	printf('batch.top')
	# 	conf = {
	# 		'db': 'user_behavior',
	# 		'queryCol': 'xz_st_batch',
	# 		'statsCol': 'xz_st_batch_date',
	# 	}

	# 	# top好友数
	# 	pipeline = [
	# 		{'$match': {'date': self._today}},
	# 		{'$group':{'_id':{'sd':'$sd','batch':'$batch'}}},
	# 		{'$group':{'_id':{'batch':'$_id.batch'},'uv':{'$sum':1}}},
	# 	]
	# 	print(pipeline)

	# 	def mapFunc(doc,data):
	# 		print(doc)
	# 		_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
	# 		field = 'sd_uv_{}'.format(int(doc['_id']['batch']))
	# 		update =  {'$set': {field:doc['uv'], 'date': self._today}}
	# 		return _id,update,conf['statsCol'],data
	# 	mongoStats(conf,pipeline,mapFunc)

	# 	printf('batch.top', 'end')
		

Batch = batch()