# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase


class newsUser(newsBase):
	def _mongoStats(self,conf, pipeline, mapFunc=''):
		statsCol = '{}_{}'.format('user',conf['statsCol'])

		pipeline[0]['$match']['date'] = self.today

		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['tvmid'] = {'$first':'$d'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
		pipeline[1]['$group']['_id']['d'] = '$d'

		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['tvmid'] = {'$first':'$_id.d'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
			pipeline[2]['$group']['_id']['d'] = '$_id.d'
		print(pipeline)

		def mapFunc1(doc,data):
			print(doc)
			doc_id = doc['_id']

			if not doc['_id'].get('pst'):
				return '', '', '', False
			if not doc['_id'].get('d'):
				return '', '', '', False

			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False

			if  callable(mapFunc):
				_id, update, _ , _ = mapFunc(doc,data)
			else:
				update = doc
				del update['_id']
				
			_id = '{today}_{d}'.format(
					today=doc['date'],
					d=doc_id['d']
			)
			colUser =  '{pst}_{col}'.format(pst=doc_id['pst'], col=statsCol)
			update =  {'$set': update}
			#print(update)
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

	def collectStat(self,data):
		self._collectStat()

NewsUser = newsUser()