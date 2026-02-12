from cfg import cfg
from pymongo import MongoClient
import time
from datetime import date
import json
from common.funcs import funcs


def printf(flag, ifbegin='begin', msg=''):
	tpl = '{date}:{msg} ==={flag} {begin}'

	if isinstance(msg, (list, dict)):
		msg = json.dumps(msg)

	tpl = tpl.format(flag=flag,
		date=time.strftime('%Y-%m-%d %H:%M:%S'),
		msg=msg,
		begin=ifbegin)
	print(tpl)

class share(object):
		
	def source(data):
		printf('source')

		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior

		if data.get('share', '0') == '1':
			collection = db.mc_share_app
			field = 'share_'
		else:
			collection = db.mc_share
			field = ''

		today = str(date.today())
		todayTime = funcs.date2time(today)

		pipeline = []

		query = {'time': {'$gt': todayTime*1000}}

		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': {'pst': '$pst', 'd': '$d'}}})
		pipeline.append({'$group': {'_id': '$_id.pst', 'total': {'$sum': 1}}})
		print(pipeline)

		for doc in collection.aggregate(pipeline):
			print(doc)
			_id = '{source}_{date}'.format(source=doc['_id'], date=today)
			updateOne = {field+'uv': doc['total'], 'source': doc['_id'], 'date': str(today)}
			client.stats.mc_share_date.update_one(
				{'_id': _id},
				{'$set': updateOne},
				upsert=True)

		pipeline = []

		query = {'time': {'$gt': todayTime*1000}}

		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': '$pst', 'total': {'$sum': 1}}})
		print(pipeline)

		for doc in collection.aggregate(pipeline):
			print(doc)
			_id = '{source}_{date}'.format(source=doc['_id'], date=today)
			updateOne = {field+'pv': doc['total'], 'source': doc['_id'], 'date': str(today)}
			client.stats.mc_share_date.update_one(
				{'_id': _id},
				{'$set': updateOne},
				upsert=True)

		printf('source', 'end')
		pass


	def sourceByPd(data):
		printf('sourceByPlayid')

		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior

		if data.get('share', '0') == '1':
			collection = db.mc_share_app
			field = 'share_'
		else:
			collection = db.mc_share
			field = ''

		today = str(date.today())
		todayTime = funcs.date2time(today)

		pipeline = []

		query = {'time': {'$gt': todayTime*1000}}

		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': {'pst': '$pst',
											'd': '$d',
											'pd': '$pd'}}})
		pipeline.append({'$group': {'_id':
						{'pst': '$_id.pst', 'pd': '$_id.pd'}, 
						'total': {'$sum': 1}}})
		print(pipeline)

		for doc in collection.aggregate(pipeline):
			print(doc)
			source = doc['_id']['pst']
			pid = doc['_id']['pd']
			_id = '{date}_{source}_{pid}'.format(source=source, date=today, pid=pid)
			updateOne = {
				field+'uv': doc['total'],
				'source': source,
				'playbill_id': pid,
				'date': today
			}
			client.stats.mc_share_detail_date.update_one(
				{'_id': _id},
				{'$set': updateOne},
				upsert=True)

		pipeline = []

		query = {'time': {'$gt': todayTime*1000}}

		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': {'pst': '$pst',
											'pd': '$pd'},
									'total': {'$sum': 1}}})

		print(pipeline)

		for doc in collection.aggregate(pipeline):
			print(doc)

			source = doc['_id']['pst']
			pid = doc['_id']['pd']
			_id = '{date}_{source}_{pid}'.format(source=source, date=today, pid=pid)
			updateOne = {
				field+'pv': doc['total'],
				'source': source,
				'playbill_id': pid,
				'date': today
			}
			client.stats.mc_share_detail_date.update_one(
				{'_id': _id},
				{'$set': updateOne},
				upsert=True)

		printf('sourceByPlayid', 'end')


	def newUv(data):
		printf('newUv')
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior
		collection = db.mc_share_app

		currentTime = int(time.time())
		# get the prevent mininute time
		currentMinute = currentTime - currentTime % 60
		frontMinute = currentMinute - 60

		today = funcs.time2date(frontMinute)
		#todayTime = funcs.date2time(today)

		query = {'time': {'$gte': frontMinute*1000, '$lt': currentMinute*1000}}
		pipeline = []
		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': {'tvmid': '$d', 'pst': '$pst'}}})
		print(pipeline)

		data = {}
		fullTvmidList = []
		for doc in collection.aggregate(pipeline):
			tvmid = doc['_id']['tvmid']
			pst = doc['_id']['pst']
			if not data.get(pst):
				data[pst] = []
			data[pst].append(tvmid)
			fullTvmidList.append(tvmid)

		for pst, tvmidList in data.items():
			if not pst: continue
			if len(tvmidList) < 1: continue

			query = {'d': {'$in': tvmidList}, 'pst': pst, 'time': {'$lt': frontMinute*1000}}
			pipeline = []
			pipeline.append({'$match': query})
			pipeline.append({'$group': {'_id': {'tvmid': '$d', 'pst': '$pst'}}})
			pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})
			count = 0

			for doc in collection.aggregate(pipeline):
				count = len(tvmidList) - doc['total']
			print(pst, count)
			if count < 1: continue
			_id = '{source}_{date}'.format(source=pst, date=today)
			updateOne = {'source': pst, 'date': today}
			client.stats.mc_share_date.update_one(
				{'_id': _id},
				{'$set': updateOne, '$inc': {'new_share_uv': count}},
				upsert=True)

		if len(fullTvmidList) < 1:
			return

		fullTvmidList = list(set(fullTvmidList))
		query = {'d': {'$in': fullTvmidList}, 'time': {'$lt': frontMinute*1000}}
		pipeline = []
		pipeline.append({'$match': query})
		pipeline.append({'$group': {'_id': {'tvmid': '$d'}}})
		pipeline.append({'$group': {'_id': 'null', 'total': {'$sum': 1}}})
		
		count = 0 
		for doc in collection.aggregate(pipeline):
			count = len(fullTvmidList) - doc['total']
		print(pipeline, count)
		_id = '{source}_{date}'.format(source='all', date=today)
		updateOne = {'source': 'all', 'date': today}
		client.stats.mc_share_date.update_one(
				{'_id': _id},
				{'$set': updateOne, '$inc': {'new_share_uv': count}},
				upsert=True)

		printf('newUv', 'end')
		pass
