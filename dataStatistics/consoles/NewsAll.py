# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase
from cfg import cfg
import time
from datetime import date, datetime

from pymongo import MongoClient,ReadPreference

class newsAll(newsBase):

	def _mongoStats(self,conf, pipeline, mapFunc=''):
		pipeline[0]['$match']['date'] = self.today
		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'

		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']

			if not doc['_id'].get('pst'):
				return '', '', '', False

			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False

			if  callable(mapFunc):
				_id,update, _, data = mapFunc(doc,data)
			else:
				_id = doc['date']
				update = doc
				del update['_id']
			colUser =  '{pst}_{col}'.format(pst=doc_id['pst'], col=conf['statsCol'])
			update =  {'$set': update}
			return _id,update,colUser,data
		mongoStats(conf,pipeline,mapFunc1)


	def pageStat(self,data):
		self._listUv()
		self._detailPv()
		self._detailDu()
		self._detailAids()
		self._detailAward()

	def shareStat(self,data):
		self._shareStat()
		self._shareDetail()

	#icon pv uv 统计
	def iconStat(self, data):
		printf('iconStat')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_event_icon',
			'statsCol': 'mc_icon_date',
		}
		#pv
		pipeline = [
			{'$match': {'date': self.today }},
			{'$group': {'_id': {'type': '$type', 'iid': '$iid'}, 
						'ititle': {'$first': '$ititle'},
						'date': {'$first': '$date'},
						'pv': {'$sum': 1}}}
		]
		print(pipeline)

		def mapFunc(doc,data):
			_id = "{date}_{type}_{id}".format(date=doc['date'], type=doc['_id']['type'], id=int(doc['_id']['iid']))
			update = {
				'type': doc['_id']['type'],
				'iid': doc['_id']['iid'],
			}
			update = {**update, **doc}
			del update['_id']
			return _id, {'$set':update},'',''
		mongoStats(conf,pipeline,mapFunc)

		#uv
		pipeline = [
			{'$match': {'date': self.today }},
			{'$group': {'_id': {'type': '$type', 'iid': '$iid', 'd': '$d'},
						'ititle': {'$first': '$ititle'},
						'date': {'$first': '$date'}
			}},
			{'$group': {'_id': {'type': '$_id.type', 'iid': '$_id.iid'}, 
						'ititle': {'$first': '$ititle'},
						'date': {'$first': '$date'},
						'uv': {'$sum': 1}}}
		]
		print(pipeline)

		mongoStats(conf,pipeline,mapFunc)
		printf('iconStat','end')

	""" 用户事件数据统计 """
	def collectStat(self,data):
		printf('collectStats')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_event_behavior',
			'statsCol': 'mc_user_event_date',
		}
		#pv
		pipeline = [
			{'$match': {'date': self.today}},
			{'$group': {'_id': {'event': '$event', 'co': '$co'}, 
						'date': {'$first': '$date'},
						'pv': {'$sum': 1}}}
		]
		def mapFunc(doc,data):
			_id = "{date}_{event}_{type}".format(date=doc['date'], event=doc['_id']['event'], type=doc['_id']['co'])
			update = {
				'event': doc['_id']['event'],
				'type': doc['_id']['co'],
			}
			update = {**update, **doc}
			del update['_id']
			return _id, {'$set': update},'',''
		mongoStats(conf,pipeline,mapFunc)

		#uv
		pipeline = [
			{'$match': {'date': self.today}},
			{'$group': {'_id': {'event': '$event', 'co': '$co', 'd': '$d'}, 
						'date': {'$first': '$date'}}},
			{'$group': {'_id': {'event': '$_id.event', 'co': '$_id.co'}, 
						'date': {'$first': '$date'},
						'uv': {'$sum': 1}}}
		]
		mongoStats(conf,pipeline,mapFunc)

		#被操作的对象数
		pipeline = [
			{'$match': {'date': self.today}},
			{'$group': {'_id': {'event': '$event', 'co': '$co', 'aid': '$aids'}, 
						'date': {'$first': '$date'}}},
			{'$group': {'_id': {'event': '$_id.event', 'co': '$_id.co'}, 
						'date': {'$first': '$date'},
						'obj_nums': {'$sum': 1}}}
		]
		mongoStats(conf,pipeline,mapFunc)
		printf('collectStats','end')

	""" 以文章为维度求每天的UV
		该量可能会非常大，在脚本运行时需要注意	
		10分钟更新一次	
	"""
	def newsUvStat(self,data):
		printf('newsUvStats')
		currentTime = int(time.time())
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'art_date',
		}
		#uv
		pipeline = [
			{'$match': {'event':'page_load', 'cn': 'app'}},
			{'$group': {'_id': {'aid': '$aid', 'd': '$d'}, 
						'date': {'$first': '$date'}}},
			{'$group': {'_id': {'aid': '$_id.aid'}, 
						'date': {'$first': '$date'},
						'app_uv':{'$sum': 1}
			}}
		]
		def mapFunc(doc,data):
			_id = "{date}_{aid}".format(date=doc['date'], aid=int(doc['_id']['aid']))
			update = {'aid': doc['_id']['aid']}
			update = {**update, **doc}
			del update['_id']
			return _id,update,''
		self._mongoStats(conf,pipeline,mapFunc)
		printf('newsUvStats','end')


	""" 以文章为维度求每天的PV
		会以增量形式进行统计,要防止并发
	"""
	def newsPvStat(self,data):
		printf('newStats')
		currentTime = int(time.time())
		minute = currentTime - currentTime%60
		prevMinute = minute - 60
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'art_date',
		}
		#app pv
		pipeline = [
			{'$match': {
				'event':'page_load', 
				'cn': 'app', 
				'time': {'$gte': prevMinute*1000, '$lt': minute*1000}
			}},
			{'$group': {'_id': {'aid': '$aid', 'pst': '$pst'}, 
						'date': {'$first': '$date'},
						'cn': {'$first': '$cn'},
						'pv':{'$sum': 1}
			}}
		]
		print(pipeline)
		_id_list = {}
		_date_id_list = {}
		updateData = {}
		def mapFunc(doc,data):
			aid = doc['_id']['aid']
			pst = doc['_id']['pst']
			_id = "%s_%d".format(doc['date'], aid)
			if doc['cn'] != 'app':
				doc['cn'] = 'out'

			if not updateData.get('date'):
				updateData['date'] = doc['date']
				updateData['up_pv_time'] = minute
				updateData['cn'] = doc['cn']
			
			if not _id_list.get(pst):
				_id_list[pst] = {}

			if not _date_id_list.get(pst):
				_date_id_list[pst] = {}


			if not _id_list[pst].get(doc['pv']):
				_id_list[pst][doc['pv']] = []

			if not _date_id_list[pst].get(doc['pv']):
				_date_id_list[pst][doc['pv']] = []

			_id_list[pst][doc['pv']].append(aid)
			_date_id_list[pst][doc['pv']].append(_id)

			return _id,{},'',False


		def reduceFunc(client):
			db_read = client.get_database('stats', read_preference=ReadPreference.SECONDARY)
			print('articla all stat begin========================')
			for pst, pvs in _id_list.items():
				colName =  '{}_art'.format(pst)
				incField = updateData['cn'] + '_pv'
				print ('---------------------')
				for pv, id_list in  pvs.items():
					print ('pst:',pst, ',pv:', pv, ',total', len(id_list))
					filters = {'_id': {'$in': id_list}}
					updates = {'$set':{
						'up_pv_time': updateData['up_pv_time'],
					},'$inc': {
						incField: pv
					}}
					updateSess = client['stats'][colName].update_many(filters, updates)
					print('update ', updateSess.modified_count)

					exists_id_list = []
					for res in db_read[colName].find({'_id': {'$in': id_list}} , {'_id':1}):
						exists_id_list.append(res['_id'])
					not_exists_id_list = list(set(id_list) ^ set(exists_id_list))

					insertData = []
					for _id in not_exists_id_list:
					 	insertData.append({
					 		'_id': _id,
					 		incField: 1,
					 		'up_pv_time': updateData['up_pv_time'],
					 		'date': updateData['date'],
					 	})
					print('insert {}'.format(len(insertData)))
					if len(insertData)>0:
						client['stats'][colName].insert_many(insertData, False)

			print('\narticla  date stat begin========================')
			for pst, pvs in _date_id_list.items():
				colDate = '{}_{}'.format(pst, conf['statsCol'])
				incField = updateData['cn'] + '_pv'
				print ('---------------------')
				for pv, id_list in  pvs.items():
					filters = {'_id': {'$in': id_list}}
					print ('pst:',pst, ',pv:', pv, ',total', len(id_list))

					updates = {'$set':{
						'up_pv_time': updateData['up_pv_time'],
						'date': updateData['date'],
					},'$inc': {
						incField: pv
					}}
					updateSess = client['stats'][colDate].update_many(filters, updates)

					print('update ', updateSess.modified_count)

					exists_id_list = []
					for res in db_read[colDate].find({'_id': {'$in': id_list}} , {'_id':1}):
						exists_id_list.append(res['_id'])

					not_exists_id_list = list(set(id_list) ^ set(exists_id_list))

					insertData = []
					for _id in not_exists_id_list:
					 	insertData.append({
					 		'_id': _id,
					 		incField: 1,
					 		'up_pv_time': updateData['up_pv_time'],
					 		'date': updateData['date'],
					 	})
					print('insert {}'.format(len(insertData)))
					if len(insertData)>0:
						client['stats'][colDate].insert_many(insertData, False)

		mongoStats(conf,pipeline,mapFunc,reduceFunc)

		#站外 pv
		pipeline = [
			{'$match': {
				'event':'page_load', 
				'cn': {'$ne': 'app'}, 
				'time': {'$gte': prevMinute*1000, '$lt': minute*1000}
			}},
			{'$group': {'_id': {'aid': '$aid', 'pst': '$pst'}, 
						'date': {'$first': '$date'},
						'cn': {'$first': '$cn'},
						'pv':{'$sum': 1}
			}}
		]
		print(pipeline)
		_id_list = {}
		_date_id_list = {}
		updateData = {}
		mongoStats(conf,pipeline,mapFunc,reduceFunc)
		printf('newStats','end')

	def eventAdHandler(self,data):
		printf('newStats')
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.get_database('user_behavior', read_preference=ReadPreference.SECONDARY)
		db_write = client.user_behavior
		collection = db.mc_event_click

		query = {'status': 0, 'type': 'ad_video'}
		limit = 100
		colHandler = 'mc_ad_video_show'
		j = 0
		i = 0 
		while True:
			j = j + 1
			i = 0
			doc_id_list = []
			user_list = {}
			for doc in collection.find(query).limit(100):
				i = i + 1
				doc_id_list.append(doc['_id'])
				yesterday = funcs.date2PrevDate(doc['date'])
				_id = '{}_{}'.format(yesterday, doc['d'])
				user_list[_id] = {
					'd': doc['d'],
					'date': doc['date'],
					'cd': 1,
					'time': doc['time']
				}
			db_write.mc_event_click.update_many(
					{'_id': {'$in': doc_id_list}} ,
					{'$set':{'status':1}} 
			)

			show_query = {'_id': {'$in': list(user_list.keys()) }}
			for doc in db[colHandler].find(show_query):
				_id = doc['_id']
				ct = doc['cd']
				if ct % 7 == 0:
					ct = 7
				else:
					ct = ct % 7
				user_list[_id]['cd'] = ct

			for _id, user in user_list.items():
				setUpdate = {
					'cd': user['cd'],  #Continuity days
					'd': user['d'],
					'date': user['date'],
					'ISOdate':datetime.utcfromtimestamp(int(user['time']/1000)),
				}
				incUpdate = {'times':1}
				update = {'$inc': incUpdate, '$set': setUpdate}
				_id = '{}_{}'.format(user['date'], user['d'])
				db_write[colHandler].update_one({'_id':_id} , update, upsert=True)
			print ('times:',j, ',total:',i)
			time.sleep(1)
			
		printf('newStats','end')
    
    #看视频广告统计
	def eventAdStat(self, data):
		printf('eventAdStat')
		colHandler = 'mc_ad_video_show'
		today = self.today
		conf = {
			'db': 'user_behavior',
			'queryCol': colHandler,
			'statsCol': 'mc_ad_video_stat',
		}

		pipeline = [
			{'$match': {'date': today}},
			{'$group': {'_id': '$cd', 'total': {'$sum':1}}}
		]
		print(pipeline)

		def mapFunc(doc,data):
			_id = today
			field = 'ad_video_' + str(doc['_id'])
			update = {'$set': {
				field: doc['total'],
				'date': today	
			}}
			return _id, update, None,None
		mongoStats(conf,pipeline,mapFunc)

		#pv uv
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_event_click',
			'statsCol': 'mc_click_stat',
		}
		pipeline= [
			{'$match': {'date': today, 'type': 'ad_video'}},
			{'$group': {'_id': 'null', 'ad_video_pv': {'$sum':1}}}
		]

		def mapFunc(doc,data):
			_id = today
			update = doc
			del update['_id']
			update['date'] = today
			update = {'$set': update}
			return _id, update, None,None
		mongoStats(conf,pipeline,mapFunc)

		#uv 
		pipeline = [
			{'$match': {'date': today, 'type': 'ad_video'}},
			{'$group': {'_id': '$d'}},
			{'$group': {'_id': 'null', 'ad_video_uv': {'$sum':1} }}
		]
		mongoStats(conf,pipeline,mapFunc)

		printf('eventAdStat', 'end')

NewsAll = newsAll()
	





















