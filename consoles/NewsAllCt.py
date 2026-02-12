# -*- coding: utf-8 -*-
# 资讯板块统计(按资讯内容类型划分)


from common.funcs  import funcs, printf, mongoStats
from models.NewsBaseStats import newsBase


class newsAllCt(newsBase):

	def _mongoStats(self,conf, pipeline, mapFunc=''):
		pipeline[0]['$match']['date'] = self.today
		#pipeline[0]['$match']['pst'] = 'baidu_zixun'

		pipeline[1]['$group']['date'] = {'$first':'$date'}
		pipeline[1]['$group']['_id']['pst'] = '$pst'
		pipeline[1]['$group']['_id']['ct'] = '$ct'
		if len(pipeline) == 3:
			pipeline[2]['$group']['date'] = {'$first':'$date'}
			pipeline[2]['$group']['_id']['pst'] = '$_id.pst'
			pipeline[2]['$group']['_id']['ct'] = '$_id.ct'

		print(pipeline)

		def mapFunc1(doc,data):
			doc_id = doc['_id']

			if not doc['_id'].get('pst'):
				return '', '', '', False
			if not doc['_id'].get('ct'):
				return '', '', '', False

			#包含pst
			try:
				self.pstList.index( str(doc_id['pst']))
			except:
				print ('no valid pst:' , doc_id['pst'])
				return '', '', '', False


			_id = '{today}_{ct}'.format(
					today=doc['date'],
					ct=doc['_id']['ct']
			)
			if  callable(mapFunc):
				_, update, _, data = mapFunc(doc,data)
			else:
				update = doc
				del update['_id']
			colUser =  '{pst}_ct_{col}'.format(pst=doc_id['pst'], col=conf['statsCol'])
			update['ct'] = doc_id['ct']
			update =  {'$set': update}
			return _id,update,colUser,data
		mongoStats(conf,pipeline,mapFunc1)


	def pageStat(self,data):
		self._detailPv()
		self._detailDu()
		self._detailAids()
		self._detailAward()

	def shareStat(self,data):
		self._shareStat()
		self._shareDetail()

NewsAllCt = newsAllCt()