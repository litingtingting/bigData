# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase


class newsPtab(newsBase):
	def _mongoStats(self,conf, pipeline, mapFunc=''):
		statsCol = '{}_{}'.format('ptab',conf['statsCol'])

		pipeline[0]['$match']['date'] = self.today

		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['ptab'] = {'$first':'$ptab'}
		pipeline[1]['$group']['ptabid'] = {'$first':'$ptabid'}

		pipeline[1]['$group']['_id']['pst'] = '$pst'
		pipeline[1]['$group']['_id']['ptabid'] = '$ptabid'

		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['ptab'] = {'$first':'$ptab'}
			pipeline[2]['$group']['ptabid'] = {'$first':'$ptabid'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
			pipeline[2]['$group']['_id']['ptabid'] = '$_id.ptabid'
		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']

			if not doc['_id'].get('pst'):
				return '', '', '', False
			if not doc['_id'].get('ptabid'):
				return '', '', '', False

			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False

			ptabid = doc['_id']['ptabid']
			if funcs.is_number(ptabid):
				ptabid = int (ptabid)
			
			if  callable(mapFunc):
				_id, update, _ , _ = mapFunc(doc,data)
			else:
				update = doc
				del update['_id']
				
			_id = '{today}_{ptabid}'.format(
					today=doc['date'],
					ptabid=ptabid
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

NewsPtab = newsPtab()