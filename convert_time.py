# !/usr/bin/python
# -*- coding:utf-8 -*-
import time
import datetime
import re
import logging

log = logging.getLogger("DataUpdate")


def num_to_str(num):
	"""
	将NumberLong类型的时间戳转化为 '%Y-%m-%d %H:%M:%S'形势的字符串格式
	:param num:
	:return:
	"""
	try:
		num = long(num)
		if num > 1000000000000:
			num = num / 1000
		ltime = time.localtime(num)
		time_str = time.strftime('%Y-%m-%d %H:%M:%S', ltime)
		return time_str
	except:
		log.error("num_to_str convert error !")
		return ""


def str_to_num(time_str):
	"""
	将'%Y-%m-%d %H:%M:%S'形势的字符串格式转化为NumberLong类型的时间戳
	:param time_str:
	:return:
	"""
	try:
		if re.match("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", time_str):
			num_time = time.strptime(time_str, '%Y-%m-%d %H:%M:%S')
			num = long(time.mktime(num_time) * 1000)
			return num
		else:
			print "%s is error code form !", time_str
	except:
		log.error("str_to_num convert error !")
		return ""


def time_add(time_str, delta):
	"""
	按小时数增加时间
	:param time_str:
	:param delta:
	:return:
	"""
	try:
		if re.match("\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", time_str):
			num_time = datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
			add_time = datetime.timedelta(hours=delta)
			num_time += add_time
			return num_time.strftime('%Y-%m-%d %H:%M:%S')
	except:
		log.error("time add error !")
		return ""


if __name__ == '__main__':
	print str_to_num("2015-12-03 12:23:12")
	print num_to_str(1449116592321)
	print time_add("2015-12-03 12:23:12", 30)
