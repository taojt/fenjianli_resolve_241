# !/usr/bin/python
# -*- coding:utf-8 -*-
import constant
import ConfigParser
import logging

log = logging.getLogger("DataUpdate")


def init_property():
	"""
	初始化变量
	:return:
	"""
	cf = ConfigParser.RawConfigParser()
	try:
		cf.read("resume_config.conf")
		constant.min_time = cf.get("resume_info", "min_time")
		constant.max_time = cf.get("resume_info", "max_time")
		constant.start_time = cf.get("resume_info", "start_time")
	except:
		log.error("init propetry error !")


def set_config(config, value):
	"""
	修改变量并写回配置文件
	:param config:
	:param value:
	:return:
	"""
	cf = ConfigParser.RawConfigParser()
	try:
		cf.read("resume_config.conf")
		cf.set("resume_info", config, value)
		cf.write(open("resume_config.conf", "w"))
	except IOError:
		log.error("config file write back error !")
