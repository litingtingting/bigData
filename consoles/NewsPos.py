# -*- coding: utf-8 -*-
# 地区统计

from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase

class NewsPos(newsBase):
	def _mongoStats(self,conf, pipeline, mapFunc=''):
		statsCol = '{}_{}'.format('pos',conf['statsCol'])

		pipeline[0]['$match']['date'] = self.today

		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['pos'] = {'$first':'$_id.pos'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
	# 	pipeline[1]['$group']['_id']['ptabid'] = '$ptabid'

		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['pos'] = {'$first':'$_id.pos'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
	# 		pipeline[2]['$group']['_id']['ptabid'] = '$_id.ptabid'
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
				
			_id = '{today}_{province}'.format(
					today=doc['date'],
					province=doc_id.get('pos','')
				)
			colUser =  '{pst}_{col}'.format(pst=doc_id['pst'], col=statsCol)
			update =  {'$set': update}
			return _id,update,colUser,data
		mongoStats(conf,pipeline,mapFunc1)

	def PosStat(self,data):
		self._posLoad()


NewsPos = NewsPos()