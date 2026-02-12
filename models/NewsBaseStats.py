# -*- coding: utf-8 -*-
# 资讯维度统计

from cfg import cfg
from pymongo import MongoClient
import time
from datetime import date, timedelta
import json
from common.funcs  import funcs, printf, mongoStats

class newsBase(object):
	today = str(date.today())
	todayTime = funcs.date2time(today)
	pstList = cfg.newspst.get('list').split(',')

	def _mongoStats(self,conf, pipeline, mapFunc=''):
		pipeline[0]['$match']['date'] = self.today
		#pipeline[0]['$match']['pst'] = 'baidu_zixun'
		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']
			if  callable(mapFunc):
				_id, update,data = mapFunc(doc,data)
			else:
				_id = doc['date']
				update = doc
				del update['_id']
			print("=============")
			#print(doc)
			print("*********************")
			#print(update)
			colUser =  '{pst}_{col}'.format(pst=doc_id['pst'], col=conf['statsCol'])
			update =  {'$set': update}
			return _id,update,colUser,data
		mongoStats(conf,pipeline,mapFunc1)

	def pageStat(self,data):
		self._listUv()
		self._listPv()
		self._detailPv()
		self._detailDu()
		self._detailAids()
		self._detailAward()

	def shareStat(self,data):
		self._shareStat()
		self._shareDetail()

	"""  列表pv  """
	def _listPv(self):
		printf('_listPv')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_load_zixun1',
			'statsCol': 'date',
		}
		pipeline = [
			{'$match': {'page': 'list'}},
			{'$group': {'_id': {}, 'list_pv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)
		printf('_listPv', 'end')

	""" 列表UV """
	def _listUv(self):
		printf('_listUv')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_load_zixun1',
			'statsCol': 'date',
		}
		pipeline = [
			{'$match': {'page': 'list'}},
			{'$group': {'_id': {'d': '$d'}} },
			{'$group': {'_id': {}, 'list_uv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)
		printf('_listUv', 'end')

	""" 资读详情页PV&UV """
	def _detailPv(self):
		printf('_detailPv')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'date',
		}
		#pv
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load'}},
			{'$group': {'_id': {}, 'view_pv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		#uv
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load'}},
			{'$group': {'_id': {'d': '$d'}}},
			{'$group': {'_id': {}, 'view_uv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		printf('_detailPv', 'end')

	"""资读详情页停留时长和观看时长"""
	def _detailDu(self):
		printf('_detailDu')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'date',
		}

		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_leave'}},
			{'$group': {'_id': {}, 
					'view_st': {'$sum': '$st'}, 
					'view_vt':{'$sum': '$vt'},
			}}
		]
		self._mongoStats(conf,pipeline)
		printf('_detailDu', 'end')


	""" 资读详情页浏览的总文章数 """
	def _detailAids(self):
		printf('_detailAids')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'date',
		}
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load'}},
			{'$group': {'_id': {'aid':'$aid'}}},
			{'$group': {'_id': {}, 'view_news_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		printf('_detailAids', 'end')

	""" 资读详情页发放的奖励 """
	def _detailAward(self):
		printf('_detailAward')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'date',
		}
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'ring_full'}},
			{'$group': {'_id': {'sdt': '$sdt'}, 'pv': {'$sum': 1}, 'award': {'$sum' : '$sdn'}}}
		]
		def mapFunc(doc,data):
			_id = doc['date']
			if not doc['_id'].get('sdt'):
				return '', '', '', False
			update = {
				'{}_award'.format(doc['_id']['sdt']): doc['award'],
				'{}_pv'.format(doc['_id']['sdt']): doc['pv'],
				'date': doc['date']
			}
			update = {**update,**doc}
			del update['_id']
			del update['pv']
			del update['award']
			# update =  {'$set': update}
			return _id,update,'',data
		self._mongoStats(conf,pipeline,mapFunc)

		#uv
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'ring_full'}},
			{'$group': {'_id': {'d':'$d', 'sdt': '$sdt'}}},
			{'$group': {'_id': {'sdt': '$_id.sdt'}, 'uv': {'$sum': 1}}}
		]
		def mapFunc(doc,data):
			_id = doc['date']
			if not doc['_id'].get('sdt'):
				return '', '', '', False
			update = {
				'{}_uv'.format(doc['_id'].get('sdt'),''): doc['uv'],
				'date': doc['date']
			} 
			update = {**update,**doc}
			del update['_id']
			del update['uv']
			# update =  {'$set': update}
			return _id,update,'',data
		self._mongoStats(conf,pipeline,mapFunc)

		printf('_detailAward', 'end')

	""" 分享动作统计 """ 
	def _shareStat(self):
		printf('_shareStat')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_app_zixun_share',
			'statsCol': 'share_date',
		}
		pipeline = [
			{'$match': { }},
			{'$group': {'_id': {}, 'share_pv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		pipeline = [
			{'$match': { }},
			{'$group': {'_id': {'d': '$d' }} },
			{'$group': {'_id': {}, 'share_uv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		pipeline = [
			{'$match': { }},
			{'$group': {'_id': {'aid':'$aid'}}},
			{'$group': {'_id': {}, 'share_news_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		""" 各个分享渠道的分享行为统计 """
		pipeline = [
			{'$match': {} },
			{'$group': {'_id': {'d': '$d', 'cn': '$cn'}}},
			{'$group': {'_id': {'cn': '$_id.cn'}, 'total': {'$sum': 1}}}
		]
		print(pipeline)
		def mapFunc(doc,data):
			_id = doc['date']
			update =  {
				'{}_uv'.format(doc['_id']['cn']):doc['total'], 
				'date': doc['date'],
			}

			return _id,update,'',data
		self._mongoStats(conf,pipeline,mapFunc)

		pipeline = [
			{'$match': {} },
			{'$group': {'_id': {'cn':'$cn'}, 'total': {'$sum': 1}}}
		]
		print(pipeline)
		def mapFunc(doc,data):
			_id = doc['date']
			update = {
				'{}_pv'.format(doc['_id']['cn']): doc['total'], 
				'date': doc['date']
			}
			update = {**update,**doc}
			del update['_id']
			# update =  {'$set': update}
			return _id,update,'',data
		self._mongoStats(conf,pipeline,mapFunc)
		printf('_shareStat', 'end')

	""" 分享浏览页面数据统计 """
	def _shareDetail(self):
		printf('_shareDetail')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'share_date',
		}
		""" 站外流量pv统计 """
		pipeline = [
			{'$match': {'cn': {'$ne': 'app'} }},
			{'$group': {'_id': {}, 'share_view_pv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		""" 站外流量UV统计 """
		pipeline = [
			{'$match': {'cn': {'$ne': 'app'} }},
			{'$group': {'_id': {'ip': '$ip'}}},
			{'$group': {'_id': {}, 'share_view_uv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		""" 各个分享渠道的页面浏览统计 """
		pipeline = [
			{'$match': {'cn': {'$ne': 'app'} }},
			{'$group': {'_id': {'ip': '$ip', 'cn': '$cn'}}},
			{'$group': {'_id': {'cn': '$_id.cn'}, 'total': {'$sum': 1}}}
		]
		print(pipeline)
		def mapFunc(doc,data):
			_id = doc['date']
			update =  {
				'{}_view_uv'.format(doc['_id']['cn']):doc['total'], 
				'date': doc['date']
			}

			return _id,update,'',''
		self._mongoStats(conf,pipeline,mapFunc)

		pipeline = [
			{'$match': { 'cn': {'$ne': 'app'} }},
			{'$group': {'_id': {'cn':'$cn'}, 'total': {'$sum': 1}}}
		]
		print(pipeline)
		def mapFunc(doc,data):

			_id = doc['date']
			update = {
				'{}_view_pv'.format(doc['_id']['cn']): doc['total'], 
				'date': doc['date']
			}
			update = {**update,**doc}
			del update['_id']
			# update =  {'$set': update}
			return _id,update,'',''
		self._mongoStats(conf,pipeline,mapFunc)
		printf('_shareDetail', 'end')

	""" 收藏行为统计 """
	def _collectStat(self):
		printf('_collectStat')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_event_behavior',
			'statsCol': 'collect_date',
		}

		""" 收藏pv统计 """
		pipeline = [
			{'$match': {'cn': {'event':'collect'} }},
			{'$group': {'_id': {}, 'collect_pv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		""" 收藏uv统计 """
		pipeline = [
			{'$match': {'cn': {'event':'collect'} }},
			{'$group': {'_id': {'d': '$d'}} },
			{'$group': {'_id': {}, 'collect_uv': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		""" 收藏资讯数量统计 """
		pipeline = [
			{'$match': {'cn': {'event':'collect'} }},
			{'$group': {'_id': {'aid': '$aid'}} },
			{'$group': {'_id': {}, 'collect_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		printf('_collectStat', 'end')

	"""  文章加载事件的地理位置相关统计  """
	def _posLoad(self):
		printf('_posLoad')
		conf = {
			'db': 'user_behavior',
			'queryCol': 'mc_page_zixun_detail',
			'statsCol': 'province_date',
		}

		# 省份UV统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load','ip_mark':True}},
			{'$group': {'_id': {'d': '$d','pos':'$province'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'user_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		# 省份资讯总数统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load','ip_mark':True}},
			{'$group': {'_id': {'aid': '$aid','pos':'$province'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'news_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		# 省份阅读时长统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_leave','ip_mark':True}},
			{'$group': {'_id': {'vt': '$vt','pos':'$province'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'vt': {'$sum': '$_id.vt'}}}
		]
		self._mongoStats(conf,pipeline)


		conf['statsCol'] = 'city_date'
		# 城市UV统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load','ip_mark':True}},
			{'$group': {'_id': {'d': '$d','pos':'$city'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'user_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		# 城市资讯总数统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_load','ip_mark':True}},
			{'$group': {'_id': {'aid': '$aid','pos':'$city'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'news_num': {'$sum': 1}}}
		]
		self._mongoStats(conf,pipeline)

		# 城市阅读时长统计
		pipeline = [
			{'$match': {'cn': 'app', 'event': 'page_leave','ip_mark':True}},
			{'$group': {'_id': {'vt': '$vt','pos':'$city'}} },
			{'$group': {'_id': {'pos':'$_id.pos'}, 'vt': {'$sum': '$_id.vt'}}}
		]
		self._mongoStats(conf,pipeline)

		printf('_posLoad', 'end')