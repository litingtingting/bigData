# -*- coding: utf-8 -*-
# top统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date
from pymongo import MongoClient,ReadPreference
from cfg import cfg

class top(object):
	_today = str(date.today())

	""" 每日邀请好友TOP20 """
	def apprentice(self,data):
		printf('top.apprentice')

		statHost = cfg.mongodb.get('host')
		client = MongoClient(statHost)
		db = client.get_database('user_behavior', read_preference=ReadPreference.SECONDARY)
		collection = db['xz_st_apprentice']

		statDb = client['stats']
		statCollection = statDb['xz_top_apprentice_date']

		# 新增好友人数
		pipeline = [
			{'$match': {'date': self._today}},
			{'$group':{'_id':{'d':'$d','sd':'$sd'}}},
			{'$group':{'_id':{'d':'$_id.d'},'num':{'$sum':1}}},
			{'$sort':{'num':-1}},
		]
		print(pipeline)

		i = 0
		data = []
		for doc in collection.aggregate(pipeline):
			i = i + 1
			field = {'tvmid':doc['_id']['d'],'num':doc['num']}
			data.append(field)
			if i >= 20:
				break

		update = {'$set':{'toplist':data,'date':self._today}}
		statCollection.update_one({'date':self._today}, update, upsert=True)


		data1 = statCollection.find_one({'date':self._today})
		if data1:
			for raw in data1['toplist']:
				# 获得奖励金额
				reward = 0
				pipeline = [
					{'$match': {'date':self._today,'d': raw['tvmid']}},
					{'$group':{'_id':'$d','num':{'$sum':'$reward'}}},
				]
				for res in db['xz_st_batch'].aggregate(pipeline):
					reward = reward + res['num']
				raw['reward'] = reward

				# 奖励发放涉及人数	
				sd_num = 0
				pipeline = [
					{'$match': {'date':self._today,'d': raw['tvmid']}},
					{'$group':{'_id':'$d','num':{'$sum':1}}},
				]
				for res in db['xz_st_batch'].aggregate(pipeline):
					sd_num = sd_num + res['num']
				raw['sd_num'] = sd_num
			print(data1)

		update = {'$set':{'toplist':data1['toplist']}}
		statCollection.update_one({'_id':data1['_id']}, update, upsert=True)

		printf('top.apprentice', 'end')

Top = top()