# -*- coding: utf-8 -*-
# 双旦活动相关统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date

class sd(object):
	_today = str(date.today())

	def Reward(self,data):
		""" 奖励发放量统计 """
		printf('sd.Reward')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_sd_reward',
			'statsCol': 'xz_sd_reward_date',
		}

		# 砸蛋人数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','eg':'$eg'}}},
			{'$group':{'_id':{'eg':'$_id.eg'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'eg_uv_{}'.format(int(doc['_id']['eg']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 奖励类型发放量
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'rwd':'$rwd'},'num':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'reward_{}'.format(int(doc['_id']['rwd']))
			update =  {'$set': {field:doc['num'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('sd.Reward', 'end')

	def Lucky(self,data):
		""" 幸运值人数分布统计 """
		printf('sd.Lucky')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_sd_lucky',
			'statsCol': 'xz_sd_lucky_date',
		}

		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','value':'$value'}}},
			{'$group':{'_id':{'value':'$_id.value'},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=self._today)
			field = 'value_{}'.format(int(doc['_id']['value']))
			update =  {'$set': {field:doc['uv'], 'date': self._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('sd.Lucky', 'end')

Sd = sd()