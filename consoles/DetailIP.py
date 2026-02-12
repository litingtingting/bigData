# -*- coding: utf-8 -*-
# 用户IP查询存储

from cfg import cfg
from pymongo import MongoClient
import time
import re
import requests

class DetailIP(object):
	def detailFind(self):
		host = cfg.mongodb.get('host')
		client = MongoClient(host)
		db = client.user_behavior
		collection = db.mc_page_zixun_detail
		ipHost = '{host}{uri}'.format(host=cfg.common.get('api.ip'), uri='/ip/ipsearch?ipstr=')

		query = {'ip_mark':None}

		while True:
			ip_list = []
			for doc in collection.find(query).limit(100):
				# print(doc)
				ip = doc.get('ip','*')
				if re.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", ip):
					ip_list.append(ip)
				else:
					update = {'country':'*','province':'*','city':'*','area':'*','ip_mark':True}
					collection.update_one(
						{'_id': doc.get('_id')},
						{'$set': update},
						upsert=True)


			ip_list_str = ','.join(list(set(ip_list)))
			url = '{host}{ips}'.format(host=ipHost,ips=ip_list_str)
			r = requests.get(url)
			for ip,val in r.json().items():
				if ip == '':
					continue
				ary = val.split('|')
				count = len(ary)
				country = ary[0] if count >= 1 else '*'
				province = ary[1] if count >= 2 else '*'
				city = ary[2] if count >= 3 else '*'
				area = ary[3] if count >= 4 else '*'
				update = {'country':country,'province':province,'city':city,'area':area,'ip_mark':True}
				collection.update(
					{'ip':ip}, 
					{'$set':update},
					upsert=True,
					multi=True)
			time.sleep(1)
