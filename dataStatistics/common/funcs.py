# -*- coding: utf-8 -*-
import time
from cfg import cfg
from datetime import datetime, date, timedelta

from pymongo import MongoClient,ReadPreference

def printf(flag, ifbegin='begin', msg=''):
	tpl = '{date}:{msg}============={flag} {begin}'

	if isinstance(msg, (list, dict)):
		msg = json.dumps(msg)

	tpl = tpl.format(flag=flag,
		date=time.strftime('%Y-%m-%d %H:%M:%S'),
		msg=msg,
		begin=ifbegin)
	print(tpl)


def mongoStats(conf,pipline,mapFunc,reduceFunc=''):
	dbname = conf.get('dbname', 'source')
	queryCol = conf.get('queryCol')
	statsCol = conf.get('statsCol')
	statsDb = conf.get('statsDb', 'stats')

	host = cfg.mongodb.get('host')
	host = conf.get('host', host)
	statsHost = conf.get('statsHost', host)

	if not queryCol or not statsCol:
		return False

	if not pipline:
		return False

	client = MongoClient(host)
	db = client.get_database('user_behavior', read_preference=ReadPreference.SECONDARY)
	collection = db[queryCol]


	# data = {}
	client1 = False
	if host == statsHost:
		col = client[statsDb][statsCol]
		client1 = client
	else:
		client1 = MongoClient(statsHost)
		col = client1[statsDb][statsCol]

	for doc in collection.aggregate(pipline,allowDiskUse=True):
		# print(doc)
		_id,update,col_user,data = mapFunc(doc,{})
		if col_user:
			col = client1[statsDb][col_user]
		if data != False:
			if type(_id) == dict:
				query = _id
			else:
				query = {'_id':_id}
			cursor = col.update_one(query, update, upsert=True)
			print('updateField:', update)
			print('collection:', col_user,'-',statsCol)
			print('update:', cursor.modified_count)
		
	if callable(reduceFunc):
		reduceFunc(client1)

	client.close()
	if client1: client1.close()
	return True


class funcs(object):

	@staticmethod
	def redisConnect():
		host = cfg.redis.get('host')
		port = cfg.redis.get('port')
		pwd = cfg.redis.get('auth')
		pool = redis.ConnectionPool(host=host, port=port, db=0, password=pwd, encoding='utf-8', decode_responses=True)
		return redis.Redis(connection_pool=pool)

	@staticmethod
	def time2date(timeStamp):
		localTime = time.localtime(timeStamp)
		return time.strftime("%Y-%m-%d", localTime)

	@staticmethod
	def date2time(t):
		timeStruct = time.strptime(t, "%Y-%m-%d")
		return int(time.mktime(timeStruct))

	@staticmethod	
	def date2PrevDate(t):
		dt = datetime.strptime(t,'%Y-%m-%d')
		yesterday = dt + timedelta(days = -1)
		return str(yesterday).split(' ').pop(0)


	@staticmethod
	def is_number(s):
	    try:
	        int(s)
	        return True
	    except ValueError:
	    	pass
	    	
	    return False

	
