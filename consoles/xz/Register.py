# -*- coding: utf-8 -*-
# 注册人数统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date

class register(object):
	_today = str(date.today())

	def Stat(self,data):
		printf('register')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'xz_register',
			'statsCol': 'xz_register_date',
		}

		# 注册人数
		pipeline = [
			{'$match': {'date': register._today,'macaddress':{'$ne':None}}},
			{'$group':{'_id':{'macaddress':'$macaddress'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=register._today)
			update =  {'$set': {'uv':doc['uv'], 'date': register._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 微信注册人数
		pipeline = [
			{'$match': {'date': register._today,'wx':{'$ne':None}}},
			{'$group':{'_id':{'wx':'$wx'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=register._today)
			update =  {'$set': {'wx_uv':doc['uv'], 'date': register._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		# 手机号注册人数
		pipeline = [
			{'$match': {'date': register._today,'phone':{'$ne':None}}},
			{'$group':{'_id':{'phone':'$phone'}}},
			{'$group':{'_id':{},'uv':{'$sum':1}}},
		]
		print(pipeline)

		def mapFunc(doc,data):
			print(doc)
			_id = '{source}_{date}'.format(source=conf['queryCol'], date=register._today)
			update =  {'$set': {'phone_uv':doc['uv'], 'date': register._today}}
			return _id,update,conf['statsCol'],data
		mongoStats(conf,pipeline,mapFunc)

		printf('register', 'end')

Register = register()