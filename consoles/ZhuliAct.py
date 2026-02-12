# -*- coding: utf-8 -*-

from cfg import cfg
from pymongo import MongoClient
import time
from datetime import date, timedelta
import json
from common.funcs  import funcs,printf,mongoStats

class zhuliAct(object):

	today = str(date.today())
	todayTime = funcs.date2time(today)

	def pageStat(self, data):
		printf('pageStat')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_load_zhuli',
			'statsCol': 'zl_page_date',
		}
		#pv 统计
		pipeline = [
			{'$match': {'date': self.today}},
			{'$group': {'_id': {'type': '$type'}, 'num': {'$sum': 1}}}
		]

		suffix_field = 'pv'
		def mapFunc(doc,data):
			if not doc['_id'].get('type'):
				return '', '', '', False
			_id = self.today
			field = doc['_id']['type'] + suffix_field
			update = {
				'$set': {
					field: doc['num'],
					'date': self.today
				}
			}
			return _id, update, '', True

		mongoStats(conf, pipeline, mapFunc)

		#uv 统计
		pipeline = [
			{'$match': {'date': self.today}},
			{'$group': {'_id': {'type': '$type', 'd': '$d'}}},
			{'$group': {'_id': {'type': '$type'}, 'num': {'$sum': 1}}}
		]
		suffix_field  = 'uv'
		mongoStats(conf, pipeline, mapFunc)

		printf('pageStat end')

    #任务卡相关统计
	def taskStat(self,data):
		printf('taskStat')
		conf = {
			'db': 'source',
			'queryCol': 'zl_behavior',
			'statsCol': 'zl',
		}
		#uv 统计
		pipeline = [
			{'$match': {'date': self.today, 't': {'$or', ['read_task', 'zl_task']}}},
			{'$group': {'_id': {'t': '$t', 'ac': '$ac','d': '$d'}}},
			{'$group': {'_id': {'t': '$t', 'ac': '$ac'}, 'num': {'$sum': 1}}}
		]
		suffix_field = 'uv'
		def mapFunc(doc,data):
			if not doc['_id'].get('t'):
				return '', '', '', False

			if not doc['_id'].get('ac'):
				return '', '', '', False

			_id = self.today
			field = doc['_id']['ac'] + '_' + suffix_field
			update = {
				'$set': {
					field: doc['num'],
					'date': self.today
				}
			}
			colUser = doc['_id'].get('t') + '_date'
			return _id, update, colUser, True

		mongoStats(conf, pipeline, mapFunc)

		#uv 统计
		pipeline = [
			{'$match': {'date': self.today, 't': {'$or', ['read_task', 'zl_task']}}},
			{'$group': {'_id': {'t': '$t', 'ac': '$ac'}, 'num': {'$sum': 1}}}
		]
		suffix_field = 'pv'
		mongoStats(conf, pipeline, mapFunc)

		printf('taskStat end')


	def collectWdStat(self,data):
		printf('collectWdStat')
		conf = {
			'db': 'source',
			'queryCol': 'zl_behavior',
			'statsCol': 'zl_colwd_date',
		}

		pipeline = [
			{'$match': {'date': self.today, 't': 'collect_wd'}},
			{'$group': {'_id': {'ac': '$ac'}, 'num': {'$sum': 1}}}
		]

		def mapFunc(doc,data):
			if not doc['_id'].get('ac'):
				return '', '', '', False

			_id = self.today
			field = doc['_id']['ac'] 
			update = {
				'$set': {
					field: doc['num'],
					'date': self.today
				}
			}
			return _id, update, '', True

		mongoStats(conf, pipeline, mapFunc)
		printf('collectWdStat end')


	def zlFullStat(self,data):
		printf('zlFullStat')
		conf = {
			'db': 'source',
			'queryCol': 'zl_behavior',
			'statsCol': 'zl_full',
		}
		pipeline = [
			{'$match': {'t': 'wd_success'}},
			{'$group': {'_id': 'null', 'num': {'$sum': 1}}}
		]

		def mapFunc(doc,data):
			_id = 'full'
			update = {
				'$set': {
					'wd_success_num': doc['num'],
					'date': self.today
				}
			}
			return _id, update ,'' , data

		mongoStats(conf, pipeline, mapFunc)

		#领取红包次数，和红包金额
		pipeline = [
			{'$match': {'t': 'red_packets'}},
			{'$group': {'_id': 'null', 'pv': {'$sum': 1}, 'award': {'$sum', '$award'}}}
		]
		def mapFunc(doc,data):
			_id = 'full'
			update = {
				'$set': {
					'red_packets_pv': doc['pv'],
					'red_packets_award': doc['award'],
					'date': self.today
				}
			}
			return _id, update ,'' , data
		mongoStats(conf, pipeline, mapFunc)

		#领取红包人数
		pipeline = [
			{'$match': {'t': 'red_packets'}},
			{'$group': {'_id': {'d': '$d'}}},
			{'$group': {'_id': 'null', 'uv': {'$sum',1}}},
		]
		def mapFunc(doc,data):
			_id = 'full'
			update = {
				'$set': {
					'red_packets_uv': doc['uv'],
				}
			}
			return _id, update ,'' , data
		mongoStats(conf, pipeline, mapFunc)
		printf('zlFullStat end')




















		










