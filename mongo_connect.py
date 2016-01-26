# !/usr/bin/python
# -*- coding:utf-8 -*-
import pymongo
from pymongo import MongoClient
import logging
log =logging.getLogger("DataUpdate")


def connect(database, collection, user="", password="", host="127.0.0.1", port=27017):
	"""
	mongo 连接方法，返回连接游标
	:param database:
	:param collection:
	:param user:
	:param password:
	:param host:
	:param port:
	:return:
	"""
	# 连接数据库地址、端口,带有用户名和密码，默认为空
	try:
		if user != "":
			conn = MongoClient(host, int(port))
			auth = conn["admin"]
			auth.authenticate(user, password)
		else:
			conn = MongoClient(host, int(port))
		# 连接数据库
		db = conn[database]
		coll = db[collection]
		return coll
	# 连接超时， 抛出异常
	except pymongo.errors.ServerSelectionTimeoutError, e:
		log.error(e.message)
	# 其他异常错误
	except:
		log.error("mongo connect error.")
	finally:
		conn.close()



if __name__ == '__main__':
	coll = connect("test", "test")
	db_cursor = coll.find({"name": "Tom"})
	for i in db_cursor:
		print i

