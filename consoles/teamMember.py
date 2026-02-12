
from pymongo import MongoClient, DESCENDING
from cfg import cfg

class teamMember(object):

	mClient = MongoClient(cfg.mongodb.get('host'))

	db = mClient.source

	@staticmethod
	def loadData(data):
		print ('loaddata')
