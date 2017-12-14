#!/usr/bin/python3
# -*- coding: UTF-8 -*-


#数据库
import pymysql
#http 库
import httplib
#Json 数据解析
import json
#timestamp
import time  
import datetime  
#线程
import thread
import math

def initDB():
	# 打开数据库连接
	ip = "localhost"
	user = "root"
	password = "123456a!"
	dbname = "tradelj"
	db = pymysql.connect(ip,user,password,dbname )
	return db

#数据库操作函数
def insertSql(db,sqlstr):
	cursor = db.cursor()
	try:
  		# 执行sql语句
   		cursor.execute(sqlstr)
   		# 提交到数据库执行
   		db.commit()
   		return 0
	except:
   		# 如果发生错误则回滚
   		print("Failed insert!")
   		db.rollback()
   		return -1;

def selectSql(db,sqlstr):
	cursor = db.cursor()
	try:
  		# 执行sql语句
   		cursor.execute(sqlstr)
   		#results = cursor.fetchone()
   		return cursor
	except:
   		# 如果发生错误则回滚
   		print("Select failed!")
   		db.rollback()
   		return -1

def sendrequest(urlbase,reqs):
	try:
  		conn = httplib.HTTPConnection(urlbase)
		conn.request(method="GET",url=reqs) 	
		return conn
	except:
		print("Send request failed!")
   		return -1;

def savePairs( db, webName , pairs ):
	sqlbase =  """INSERT INTO `pairs` (`tradeName`, `pairs`) VALUES """ 
	pairNum = 0
	for pair in pairs:
		pair = pair.replace('\"','')
		sqldata = "(\'%s\',\'%s\')" % ( webName ,pair )
		sql = sqlbase+sqldata
		#print(sql)
		selectSqlstr = "SELECT count(*) FROM `pairs` WHERE tradeName = \'%s\' and pairs = \'%s\'" % ( webName , pair )
		#print(selectSqlstr)
		cursor = selectSql( db, selectSqlstr )
		if cursor == -1:
			print("Select Error!")
			return -1
		results = cursor.fetchone()
		#print(results[0])
		if results[0] == 0 :  #数据库已经包含相应数据了，跳过
			#print("Is NULL ,Need insert")
			insertSql(db,sql)
			pairNum = pairNum + 1
		else:
			#insertSql(db,sql)
			#print("Already include the data!!")
			continue
	print("%d pairs support !!" % pairNum)

#获取系统支持的所有交易对
def getPairs(urlbase, reqrul):
	reqs = "http://%s%s"%(urlbase,reqrul)
	print(reqs)

	conn = sendrequest(urlbase,reqs)
	if conn == -1:
		return -1
	try:
		response = conn.getresponse()
		if response.status != 200 :
			return -1
	except:
		print("Get getresponse failed!")
   		return -1;
	res= response.read()
	res = res.replace('[', '')
	res = res.replace(']','')
	pairs = res.split(',')
	#print(res)
	return pairs # list

def getPairsID( db , tradeName):
	sqlgetpairsID = "SELECT `pairs_id` FROM `pairs` WHERE pairs = \'%s\' " % tradeName
	#print(sqlgetpairsID)
	cursor = selectSql( db, sqlgetpairsID )
	if cursor == -1:
		return -1
	results = cursor.fetchone()
	#print(results)
	if len(results) == 0:
		return -1
	return results[0]

def saveMarketInfo(db, data):
	if data['result'] == "true":
		datainfos = data['pairs']
		marketinfoNum = 0
		for datainfo in datainfos:
			coinTrade = datainfo.keys()
			#print(coinTrade)
			if( len( coinTrade ) != 1 ):
				#error
				return -1
			else:
				pairsID = getPairsID( db , coinTrade[0])
				if pairsID == -1:
					return -1
				selectSqlstr = "SELECT count(*) FROM `marketinfo` WHERE pairsID = %d " % pairsID
				#print(selectSqlstr)
				cursor = selectSql( db, selectSqlstr )
				if cursor == -1:
					return -1
				results = cursor.fetchone()
				if results[0] == 0 :
					#print(results[0])
					marketdata = datainfo[coinTrade[0]]
					keys = marketdata.keys()
					sqlbase =  "INSERT INTO `marketinfo` (`pairsID`, `%s`,`%s`,`%s`) VALUES  " % ( keys[0] , keys[1],keys[2] )
					sqldata = "(%d,%f, %f,%f)" % ( pairsID ,marketdata[keys[0]], marketdata[keys[1]] ,marketdata[keys[2]] ) 
					sqlstr = sqlbase + sqldata
					#print(sqlstr)
					insertSql(db,sqlstr)
					#print("%s %s"%(coinTrade[0],marketdata))
					marketinfoNum = marketinfoNum + 1
				else:
					#应该是要更新数据的，现在不需要，先跳过
					continue
		#print("%d marketInfo insert into mysql" % marketinfoNum)
		return 0
	else:
		print("Cant get the marketInfo!")
		return -1

#获取市场信息
def getMarketInfo(urlbase, reqrul):
	reqs = "http://%s%s"%(urlbase,reqrul)
	print(reqs)
	conn = sendrequest(urlbase,reqs)
	if conn == -1:
		return -1
	try:
		response = conn.getresponse()
		if response.status != 200 :
			return -1
	except:
		print("Get getresponse failed!")
   		return -1;
	res= response.read().decode('utf-8')
	data = json.loads(res)
	return data

def saveTickerInfo(db, data,timestamp):
	#timestamp = int(time.time())
	keys = data.keys()
	for key in keys:
		pairsID = getPairsID( db , key)
		if pairsID == -1:
			return -1
		signaldata = data[key]
		if signaldata['result'] == 'true':
			del	signaldata['result']
		else:
			print("Something is wrong !")
			del	signaldata['result']
		signalkeys = signaldata.keys()
		sqlbase =  "INSERT INTO `tickerinfo` (`pairsID`, `%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`%s`,`timestamp`) VALUES  " \
					% ( signalkeys[0],signalkeys[1],signalkeys[2],signalkeys[3],signalkeys[4],signalkeys[5],signalkeys[6],signalkeys[7] )

		sqldata = "(%d,%f, %f,%f,%f, %f,%f,%f, %f,%d)" % ( pairsID, float(signaldata[signalkeys[0]]),float(signaldata[signalkeys[1]]),\
					float(signaldata[signalkeys[2]]),float(signaldata[signalkeys[3]]),float(signaldata[signalkeys[4]]),float(signaldata[signalkeys[5]]),\
					float(signaldata[signalkeys[6]]),float(signaldata[signalkeys[7]]),timestamp)
		sqlstr = sqlbase + sqldata
		insertSql(db,sqlstr)
		#print(signalkeys)

def getTickerInfo(urlbase, reqrul):
	reqs = "http://%s%s"%(urlbase,reqrul)
	print(reqs)
	conn = sendrequest(urlbase,reqs)
	if conn == -1:
		return -1
	try:
		response = conn.getresponse()
		if response.status != 200 :
			return -1
	except:
		print("Get getresponse failed!")
   		return -1;
	res= response.read().decode('utf-8')
	data = json.loads(res)
	#print(data)
	return data


def saveMarketDeep(db, data,timestamp):
	#timestamp = int(time.time())
	keys = data.keys()
	sqldata = []
	for key in keys:
		pairsID = getPairsID( db , key)
		if pairsID == -1:
			return -1
		signaldata = data[key]
		if signaldata['result'] == 'true':
			del	signaldata['result']
		else:
			print("Something is wrong !")
			del	signaldata['result']
		ordertypes = signaldata.keys()
		#print(ordertypes)
		for ordertype in ordertypes:
			orderdata = signaldata[ordertype]
			if ordertype == 'asks':
				typeOd = 0
			else:
				typeOd = 1
			i = 0
			for idata in orderdata:
				#print("==%f %f" % (float(idata[0]),float(idata[1])))
				#sqlbase =  "INSERT INTO `bookorders` (`coinpair`, `type`,`oderId`,`price`,`amount`,`timestamp`) VALUES  "
				sqldata.append( "(%d,\'%d\',%d, %f,%f,%d)" % ( pairsID, typeOd,i ,float(idata[0]),float(idata[1]),timestamp))
				#if i == len(orderdata)-1:
				#	sqldata = sqldata + "(\'%s\',\'%s\',%d, %f,%f,%d) " % ( key, ordertype,i ,float(idata[0]),float(idata[1]),timestamp)
				#else:
				#	sqldata = sqldata + "(\'%s\',\'%s\',%d, %f,%f,%d) ," % ( key, ordertype,i ,float(idata[0]),float(idata[1]),timestamp)
				#sqlstr = sqlbase + sqldata
				#print(sqlstr)
				#insertSql(db,sqlstr)
				i = i + 1
	stepup = 100
	steps = int(math.ceil(len(sqldata)/stepup))
	sqlbase =  "INSERT INTO `bookorders` (`pairsID`, `type`,`oderId`,`price`,`amount`,`timestamp`) VALUES  "
	for step in range(0,steps):
		sqlstr = ""
		stratId = step*stepup
		if len(sqldata)%stepup != 0 and step == (steps-1):
			endId = len(sqldata)
		else:
			endId = (step+1)*stepup
		#print("Start index %d, end index %d" % (stratId,endId))
		for i in range(stratId, endId):
			if i != endId-1:
				sqlstr = sqlstr + sqldata[i] + ",\n"
			else:
				sqlstr = sqlstr + sqldata[i]
		insertsql = sqlbase + sqlstr
		#print(insertsql)
		insertSql(db,insertsql)
	#print("Total corder sqldata len %d" % len(sqldata))
	#for data in sqldata:
	#	print(data)
	

def getMarketDeep(urlbase, reqrul):
	reqs = "http://%s%s"%(urlbase,reqrul)
	print(reqs)
	conn = sendrequest(urlbase,reqs)
	if conn == -1:
		return -1
	try:
		response = conn.getresponse()
		if response.status != 200 :
			return -1
	except:
		print("Get getresponse failed!")
   		return -1;
	res= response.read().decode('utf-8')
	data = json.loads(res)
	#print(data)
	return data

#https://gate.io/api2 接口来源
#获取比特儿的网站数据
def gateioData(db):
	urlbase = "data.gate.io"
	webName = "gate.io"

	#获取比特儿支持的货币交易种类
	reqrulPair = "/api2/1/pairs"
	pairs = getPairs( urlbase , reqrulPair )
	savePairs(db,webName ,pairs)
	#获取市场订单参数
	reqrulMarketInfo = "/api2/1/marketinfo"
	markerinfos = getMarketInfo(urlbase, reqrulMarketInfo)
	saveMarketInfo(db,markerinfos)
	while True:

		#交易市场详细行情
		#需要定期查询10s查询一次
		timestampstart = int(time.time())
		tickerurl = "/api2/1/tickers/"
		tickerinfos = getTickerInfo(urlbase, tickerurl)
		#获取市场深度
		print("saveMarkerDeep timestamp %s " % time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
		marketdeepurl = "/api2/1/orderBooks/"
		marketdeepInfos = getMarketDeep(urlbase, marketdeepurl)
		if marketdeepInfos == -1 or tickerinfos == -1 :
			print("Error!!!")
			continue
		saveTickerInfo(db, tickerinfos,timestampstart)
		saveMarketDeep(db, marketdeepInfos,timestampstart)
		timestampend = int(time.time())
		print("current cycle need %d seconds" % (timestampend-timestampstart))
		timeused = timestampend-timestampstart
		if timeused >= 10:
			continue
		else:
			time.sleep(60-timeused)

def main():
	db = initDB()
	gateioData(db)
	db.close

if __name__ == "__main__":
    # execute only if run as a script
    main()