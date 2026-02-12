# -*- coding: utf-8 -*-
# 资讯Cat统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase


class newsCat(newsBase):

	def _mongoStats(self,conf, pipeline, mapFunc=''):
		pipeline[0]['$match']['date'] = self.today
		pipeline[1]['$group']['cats'] = {'$first':'$cats'}
		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
		pipeline[1]['$group']['_id']['catid'] = '$catid'
		if len(pipeline) == 3:
			pipeline[2]['$group']['cats'] = {'$first':'$cats'}
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
			pipeline[2]['$group']['_id']['catid'] = '$_id.catid'
		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']
			if not doc['_id'].get('pst'):
				return '', '', '', False
			if not doc['_id'].get('catid'):
				return '', '', '', False
			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False

			cat_id = doc['_id']['catid']
			if funcs.is_number(cat_id):
				cat_id = int (cat_id)
			
			_id = '{today}_{catid}'.format(today=doc['date'],catid=cat_id)
			if  callable(mapFunc):
				_, update, _, data = mapFunc(doc,data)
			else:
				update = doc
				del update['_id']
			colUser =  '{pst}_cat_{col}'.format(pst=doc_id['pst'], col=conf['statsCol'])
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

NewsCat = newsCat()