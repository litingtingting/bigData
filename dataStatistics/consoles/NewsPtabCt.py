# -*- coding: utf-8 -*-
# 资讯板块统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase


class newsPtabCt(newsBase):
	def _mongoStats(self,conf, pipeline, mapFunc=''):
		statsCol = '{}_{}'.format('ptab_ct',conf['statsCol'])

		pipeline[0]['$match']['date'] = self.today

		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['ptab'] = {'$first':'$ptab'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
		pipeline[1]['$group']['_id']['ct'] = '$ct'
		pipeline[1]['$group']['_id']['ptabid'] = '$ptabid'

		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['ptab'] = {'$first':'$ptab'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
			pipeline[2]['$group']['_id']['ct'] = '$_id.ct'
			pipeline[2]['$group']['_id']['ptabid'] = '$_id.ptabid'
		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']

			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False
			
			if  callable(mapFunc):
				_id, update,_,data = mapFunc(doc,data)
			else:
				update = doc
				del update['_id']
				
			_id = '{today}_{ptabid}_{ct}'.format(
					today=doc['date'],
					ptabid=doc_id.get('ptabid',0),
					ct=doc_id.get('ct','')
				)
			colUser =  '{pst}_{col}'.format(pst=doc_id['pst'], col=statsCol)
			update['ct'] = doc_id.get('ct','')
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

NewsPtabCt = newsPtabCt()