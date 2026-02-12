# -*- coding: utf-8 -*-
import os
import configparser

class Cfg(object):
    def __init__(self):
        # conf.ini文件路径必须是绝对路径，否则crontab执行脚本时会报conf.ini找不到的错误
        # 获取conf.ini绝对路径的方式如下：
        #realpath = os.path.split(os.path.realpath(__file__))[0]
        try:
            realpath = os.environ['SPARK_PY_PATH']
        except:
            realpath = os.path.split(os.path.realpath(__file__))[0]
            
        iniFile = os.path.join(realpath, 'conf/conf.ini')
        if not os.path.isfile(iniFile):
            raise ValueError('{} is not existed.'.format(iniFile))
        self._conf = configparser.ConfigParser()
        self._conf.read(iniFile,encoding='utf-8-sig')

    def __getattr__(self, name):
        return self._conf[name]

cfg = Cfg()