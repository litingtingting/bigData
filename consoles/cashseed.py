# -*- coding: utf-8 -*-

from cfg import cfg
from pymongo import MongoClient,ReadPreference
import time
from datetime import date, timedelta,datetime
import json
from common.funcs  import funcs,printf

class Cashseed(object):

	initDate = '2018-09-20 15'


	#将历史所有的数据都初始化一下
	def init(self, data):
		printf('Cashseed Init')
		initTime = 1537426800
		host = cfg.mongodb.get('cashseed.host')
		client = MongoClient(host)
		db = client.cash_seed
		db_read = client.get_database('cash_seed', read_preference=ReadPreference.SECONDARY)
		
		for index in range(0,99):
			startT = time.time()
			print ('col: ', index)
			col = db_read['user_cash_log_{}'.format(int(index))]
			pipline = [
				{'$match': {'time':{'$lt': initTime}}},
				{'$group': {
					'_id': {'tvmid': '$tvmid', 'act_type': '$act_type'},
					'times':{'$sum':1},
					'award':{'$sum':'$num'},
				}}
			]
			statCol = db['user_acttype_{}'.format(int(index))]
			j = 1 
			for doc in col.aggregate(pipline):
				print('doc:', j)
				print(doc)
				doc_id = doc['_id']

				if not doc_id.get('tvmid'):
					continue;
				if not doc_id.get('act_type'):
					continue;

				incUpdate =  {'times':doc['times'], 'num': doc['award']}
				setUpdate = {'tvmid': doc_id['tvmid'], 'act_type': doc_id['act_type']}
				_id = '{}_{}'.format(doc_id['tvmid'], int(doc_id['act_type']))
				update = {'$inc': incUpdate, '$set': setUpdate}
				statCol.update_one({'_id': _id}, update, upsert=True)
				j = j + 1
			print ('col:', index, ', executeTime:', int(time.time()-startT) , 's')

		client.close()
		printf('Cashseed Init','end')

cashseed = Cashseed()




