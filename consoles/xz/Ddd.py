# -*- coding: utf-8 -*-
# 打豆豆统计


from common.funcs  import funcs, printf, mongoStats
from cfg import cfg
from pymongo import MongoClient,ReadPreference
from datetime import date
import time

class ddd(object):
    _today = str(date.today())
    _todayTime = funcs.date2time(_today)
    _nowTime = int(time.time())

    def pu(self,data):
        printf('Ddd.pu')
        conf = {
            'db': 'user_behavior',
            'queryCol': 'xz_ddd_pvuv',
            'statsCol': 'xz_ddd_pvuv_date',
        }

        # UV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'d':'$d','t':'$t'}}},
            {'$group':{'_id':{'t':'$_id.t'},'uv':{'$sum':1}}},
        ]
        print(pipeline)

        def mapFunc(doc,data):
            print(doc)
            _id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
            if doc['_id']['t'] is None:
                doc['_id']['t'] = 0
            field = '{}_uv'.format(int(doc['_id']['t']))
            update =  {'$set': {field:doc['uv'], 'date': self._today}}
            return _id,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        # PV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'t':'$t'},'pv':{'$sum':1}}},
        ]
        print(pipeline)

        def mapFunc(doc,data):
            print(doc)
            _id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
            if doc['_id']['t'] is None:
                doc['_id']['t'] = 0
            field = '{}_pv'.format(int(doc['_id']['t']))
            update =  {'$set': {field:doc['pv'], 'date': self._today}}
            return _id,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        printf('Ddd.pu', 'end')

    def beat(self,data):
        printf('Ddd.beat')
        conf = {
            'db': 'user_behavior',
            'queryCol': 'xz_ddd_beat',
            'statsCol': 'xz_ddd_beat_date',
        }

        # UV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'d':'$d','t':'$t','is_new':'$is_new'}}},
            {'$group':{'_id':{'t':'$_id.t','is_new':'$_id.is_new'},'uv':{'$sum':1}}},
        ]
        print(pipeline)

        def mapFunc(doc,data):
            print(doc)
            _id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
            if doc['_id']['t'] is None:
                doc['_id']['t'] = 0
            field = '{}_{}_uv'.format(int(doc['_id']['t']),int(doc['_id']['is_new']))
            update =  {'$set': {field:doc['uv'], 'date': self._today}}
            return _id,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        # PV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'t':'$t','is_new':'$is_new'},'pv':{'$sum':1}}},
        ]
        print(pipeline)

        def mapFunc(doc,data):
            print(doc)
            _id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
            if doc['_id']['t'] is None:
                doc['_id']['t'] = 0
            field = '{}_{}_pv'.format(int(doc['_id']['t']),int(doc['_id']['is_new']))
            update =  {'$set': {field:doc['pv'], 'date': self._today}}
            return _id,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        # Count
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'t':'$t','is_new':'$is_new'},'num':{'$sum':'$num'}}},
        ]
        print(pipeline)

        def mapFunc(doc,data):
            print(doc)
            _id = '{source}_{date}'.format(source=conf['queryCol'] ,date=self._today)
            if doc['_id']['t'] is None:
                doc['_id']['t'] = 0
            field = '{}_{}_num'.format(int(doc['_id']['t']),int(doc['_id']['is_new']))
            update =  {'$set': {field:doc['num'], 'date': self._today}}
            return _id,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        printf('Ddd.beat', 'end')

Ddd = ddd()