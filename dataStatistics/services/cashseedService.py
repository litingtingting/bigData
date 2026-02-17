import json

def mapFunc(x):
	return json.loads(x[1])

def  mapActType(x):
	return (x.act_type, x.num)

def reduceActType(x, y):
	return x+y

def mapTvmid(x):
	return (x.tvmid, int(x.num) )

def mapTvmidActType(x):
	return (x.tvmid+"_"+x.act_type,x.num)

def mapValues(x):
	return (x, 1)

def reduceByValues(x, y):
	return (x[0] + y[0] , x[1] + y[1])

class cashseedService(object):
	pass