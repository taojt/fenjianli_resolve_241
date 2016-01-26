# !/usr/bin/python
# -*- coding:utf-8 -*-
import re


def handle_work_year(work_year):
	"""
	处理工作时间，统一化为标准格式
	:param work_year:
	:return:
	"""
	if u"暂时没有工作经验" in work_year or u"应届" in work_year or u"实习" in work_year or u"毕业生" in work_year:
		return "0"
	if re.match("(\d+)\s*年(\d+)\s*个?月", work_year):
		temp = re.match("(\d+)\s*年(\d+)\s*个?月", work_year)
		s = str(int(temp.group(1)) * 12 + int(temp.group(2)))
		return s
	elif u"以上工作经验" in work_year:
		temp = re.match("(\d+)年以上工作经验", work_year)
		if temp:
			s = int(temp.group(1)) * 10
			s1 = int(s * 1.5)
			return str(s) + "-" + str(s1)
	elif re.match("(\d+)\s*年", work_year):
		temp = re.match("(\d+)\s*年", work_year)
		s = str(int(temp.group(1)) * 12)
		return s
	elif re.match("(\d+)个?月", work_year):
		temp = re.match("(\d+)个?月", work_year)
		s = str(int(temp.group(1)))
		return s
	elif re.match("\d+", work_year):
		s = str(int(work_year) *12)
		return  s
	if u"年工作经验" in work_year:
		temp = re.match("(\d+)年工作经验", work_year)
		if temp:
			s = str(int(temp.group(1)) * 12)
			return s
	if re.match(u"(\d+)-(\d+)年工作经验", work_year):
		s1 = re.match(u"(\d+)-(\d+)年工作经验", work_year).group(1)
		s2 = re.match(u"(\d+)-(\d+)年工作经验", work_year).group(2)
		return str(int(s1) * 12) + "-" + str(int(s2) * 12)
	return ""


def handle_salary(salary):
	"""
	处理工作年薪， 统一化为标准格式
	:param salary:
	:return:
	"""
	if u"面议" in salary or u"不显示职位月薪范围" in salary or u"保密" in salary:
		return u"面议"
	if u"以上" in salary:
		temp = re.match(u"(\d{4,7})(元/月)?以上", salary)
		if temp:
			s = int(temp.group(1))
			s1 = int(s * 1.5)
			return str(s) + "-" + str(s1)
	if u"以下" in salary:
		temp = re.match(u"(\d{4,7})以下", salary)
		if temp:
			s = int(temp.group(1))
			s1 = int(s * 0.8)
			return str(s1) + "-" + str(s)
	if u"到" in salary:
		if re.match(u"\d+到\d+", salary):
			return salary.replace(u"到", "-")
	if re.match(u"\d+-\d+元?/月", salary):
		return salary.strip()[:-3]

	return ""
