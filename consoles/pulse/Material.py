# -*- coding: utf-8 -*-
# 小脉生活 - 推荐统计

from common.funcs  import funcs, printf, mongoStats
from datetime import date
from pymongo import MongoClient,ReadPreference

class material(object):
    _today = str(date.today())

    def Event(self,data):
        printf('Material')
        conf = {
            'db': 'user_behavior',
            'queryCol': 'pulse_material_event',
            'statsCol': 'pulse_material_event_date',
        }

        # UV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'d':'$d','event':'$event','mid':'$mid'}}},
            {'$group':{'_id':{'event':'$_id.event','mid':'$_id.mid'},'uv':{'$sum':1}}},
        ]
        # print(pipeline)

        updateData = {}
        _id = '{source}_{date}'.format(source=conf['queryCol'],date=self._today)

        def mapFunc(doc,data):
            print(doc)
            event = doc['_id']['event']
            mid = int(doc['_id'].get('mid',0))
            if not updateData.get(event):
                updateData[event] = []

            idData = {}
            idData['mid'] = mid
            idData['uv'] = doc['uv']
            updateData[event].append(idData)

            return '','',conf['statsCol'],False

        def reduceFunc(client):
            if not updateData:
                return
            db_read = client.get_database('stats', read_preference=ReadPreference.SECONDARY)
            update = {'$set':updateData}
            db_read[conf['statsCol']].update_one({'_id':_id},update,upsert=True)
        mongoStats(conf,pipeline,mapFunc,reduceFunc)

        # PV
        pipeline = [
            {'$match': {'date': self._today}},
            {'$group':{'_id':{'event':'$event','mid':'$mid'},'pv':{'$sum':1}}},
        ]
        print(pipeline)


        def mapFunc(doc,data):
            print(doc)
            mid = int(doc['_id'].get('mid',0))
            find = '{}.{}'.format(doc['_id']['event'],'mid')
            query = {'_id':_id,find:mid}
            field = '{}.$.{}'.format(doc['_id']['event'],'pv')
            
            update =  {'$set': {field:doc['pv'], 'date': self._today}}
            return query,update,conf['statsCol'],data
        mongoStats(conf,pipeline,mapFunc)

        printf('Material', 'end')

Material = material()