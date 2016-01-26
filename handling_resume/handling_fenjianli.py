# !/usr/bin/python
# -*- coding:utf-8 -*-
__author__ = 'Tao Jiang'

from __init__ import *


def handling_fenjianli(d={}):
	"""
	解析纷简历的方法，最后返回解析好的dict 类型resume
	:param d:
	:return:
	"""
	resume = {"resume_id": "", "cv_id": "", "phone": "", "name": "", "email": "", "create_time": long(0),
			  "crawled_time": long(0), "update_time": "", "resume_keyword": "", "resume_img": "",
			  "self_introduction": "",
			  "expect_city": "", "expect_industry": "", "expect_salary": "", "expect_position": "",
			  "expect_job_type": "",
			  "expect_occupation": "", "starting_date": "", "gender": "", "age": "", "degree": "",
			  "enterprise_type": "",
			  "work_status": "", "source": "", "college_name": "", "profession_name": "", "last_enterprise_name": "",
			  "last_position_name": "", "last_enterprise_industry": "",
			  "last_enterprise_time": "", "last_enterprise_salary": "", "last_year_salary": "", "hometown": "",
			  "living": "", "birthday": "", "marital_status": "", "politics": "", "work_year": "", "height": "",
			  "interests": "", "resume_url": "", "career_goal": "", "specialty": "",
			  "special_skills": "", "drive_name": "", "country": "", "osExperience": "", "status": "0", "flag": "0",
			  "dimension_flag": False, "version": [], "keyword_id": [], "resumeUpdateTimeList": [], "educationList": [],
			  "workExperienceList": [], "projectList": [], "trainList": [], "certificateList": [], "languageList": [],
			  "skillList": [], "awardList": [],
			  "socialList": [], "schoolPositionList": [], "productList": [], "scholarshipList": []}



	# 来源
	resume["source"] = u"纷简历"

	resume["resume_id"] = str(uuid.uuid4()).replace("-", "")
	# cv_id
	if "id" in d:
		resume["cv_id"] = d.get("id")

	# 简历 url
	if "originalFilePath" in d:
		resume["resume_url"] = d.get("originalFilePath")

	# 写文件记录错误格式数据
	output = open("error_source_data.txt", "a")
	out_temp = "id: " + resume["cv_id"]

	if "contact" in d and isinstance(d["contact"], dict):
		# phone
		if "phoneNum" in d["contact"]:
			phone = d["contact"].get("phoneNum")
			if len(phone) >= 11:
				phone = phone[0:10]
				resume["phone"] = phone
		# email
		if "eMail" in d["contact"]:
			email = d["contact"].get("eMail")
			if email is not None:
				temp = email.split(" ")
				if len(temp) > 0:
					resume["email"] = temp[0]

					# 排除爬虫过程中的乱码错误数据
					if u"@" not in resume["email"] and "com" not in resume["email"] and "cn" not in resume[
						"email"] and "org" not in resume["email"]:
						resume["email"] = ""
						output.write(out_temp + " error_info: email \n")
					# 规整格式 转发用人方（1211175792@qq.com）
					if u"转发用人方" in resume["email"]:
						resume["email"] = resume["email"].replace(u"转发用人方", "").replace(u"）", "").replace(u"（",
																										  "").strip()
	# name
	if "realName" in d:
		resume["name"] = d.get("realName")

	# create_time
	resume["create_time"] = long(time.time() * 1000)

	# 爬取时间
	if "last_crawled_time" in d:
		resume["crawled_time"] = long(d["last_crawled_time"])

	# 更新时间
	if "updateDate" in d:
		resume["update_time"] = d["updateDate"]

	# 更新时间列表
	if "updateDateList" in d:
		resume["resumeUpdateTimeList"] = d.get("updateDateList")
		if resume["update_time"] != "" and resume["update_time"] not in resume["resumeUpdateTimeList"]:
			if isinstance(resume["resumeUpdateTimeList", list]):
				resume["resumeUpdateTimeList"].append(resume["update_time"])

	# 期待薪资
	if "salary" in d:
		salary = d.get("salary")
		resume["expect_salary"] = handle_salary(salary)

	# 工作时间
	if "workYear" in d:
		workYear = d.get("workYear")
		resume["work_year"] = handle_work_year(workYear)

	# 学历
	if "degree" in d:
		resume["degree"] = d.get("degree")

	# 居住地
	if "area" in d:
		resume["living"] = d.get("area").replace("-", "")

	# 年龄
	if "age" in d:
		resume["age"] = d.get("age")

	# 性别
	if "sex" in d:
		resume["gender"] = d.get("sex")

	# 最后一份工作职业
	if "job" in d:
		resume["last_position_name"] = d.get("job")

	if "name" in d:
		temp = d.get("name")
		temp2 = temp.split("|")
		if len(temp2) >= 2:
			if resume["name"] == "":
				resume["name"] = temp2[0]
			if resume["work_year"] == "":
				if u"工作经验" in temp2[1]:
					if temp2[1] == "无工作经验":
						resume["work_year"] = "0"
					else:
						resume["work_year"] = handle_work_year(temp2[1])
		if len(temp2) >= 3:
			if resume["degree"] == "":
				resume["degree"] = temp2[2]

	if "description" in d and isinstance(d["description"], dict):
		# 解析求职意向
		if "jobIntension" in d["description"] and isinstance(d["description"]["jobIntension"], dict):
			jobIntension = d["description"]["jobIntension"]
			if "city" in jobIntension and isinstance(jobIntension["city"], list):
				s = ""
				for i in range(len(jobIntension["city"])):
					s += jobIntension["city"][i] + ";"
				# 期待工作城市
				resume["expect_city"] = s.replace("-", "").strip().strip(";")

			if "job_nature" in jobIntension:
				# 期待工作类型
				temp = jobIntension.get("job_nature").replace(u"、", ",")
				# 排除错误项
				if re.match("^\d+$", temp):
					temp = ""
					output.write(out_temp + " error_info: job_nature \n")
				resume["expect_job_type"] = temp

			if "job" in jobIntension and isinstance(jobIntension["job"], list):
				s = ""
				for i in range(len(jobIntension["job"])):
					s += jobIntension["job"][i] + ","
				# 期待工作
				resume["expect_position"] = s.strip().strip(",")

			if "trade" in jobIntension and isinstance(jobIntension["trade"], list):
				s = ""
				for i in range(len(jobIntension["trade"])):
					s += jobIntension["trade"][i] + ","
				# 期待工作行业
				resume["expect_industry"] = s.strip().strip(",")

			if "state" in jobIntension:
				# 工作状态
				resume["work_status"] = jobIntension.get("state")

		# 解析工作经历
		if "work" in d["description"] and isinstance(d["description"]["work"], dict):
			work = d["description"]["work"]
			workList = {"enterprise_name": "", "position_name": "", "experience_desc": "", "start_date": "",
						"end_date": "", "enterprise_size": "", "enterprise_type": "", "work_time": "",
						"enterprise_industry": "", "salary": "", "department": "", "second_job_type": "",
						"first_job_type": ""}
			if "company" in work:
				workList["enterprise_name"] = work.get("company")
				# 最后工作公司名称
				if resume["last_enterprise_name"] == "":
					resume["last_enterprise_name"] = workList["enterprise_name"]
			if "company_type" in work:
				workList["enterprise_type"] = work.get("company_type")
			if "job" in work:
				workList["position_name"] = work.get("job")
				# 最后工作名称
				if resume["last_position_name"] == "":
					resume["last_position_name"] = workList["position_name"]
			if "job_description" in work:
				workList["experience_desc"] = work.get("job_description")
			if "trade" in work:
				temp = work.get("trade")
				temp2 = temp.split(" ")
				if len(temp2) >= 1:
					workList["enterprise_industry"] = temp2[0]
				# 最后工作行业
				if resume["last_enterprise_industry"] == "":
					resume["last_enterprise_industry"] = workList["enterprise_industry"]
			if "department" in work:
				workList["department"] = work.get("department")
			if "stime" in work:
				workList["start_date"] = work.get("stime").replace(".", "-")
				if workList["start_date"] == "0":
					workList["start_date"] = ""
			if "etime" in work:
				workList["end_date"] = work.get("etime").replace(".", "-")
				if workList["end_date"] == "0":
					workList["end_date"] = ""
			if "salary_from" in work and "salary_to" in work:
				if work.get("salary_from") != "" and work.get("salary_to") != "":
					workList["salary"] = work.get("salary_from") + "-" + work.get("salary_to")
				# 最后一份工作薪水
				if resume["last_enterprise_salary"] == "":
					resume["last_enterprise_salary"] = workList["salary"]
			if "company_size_from" in work and "salary_to" in work:
				if work.get("company_size_to") != "0":
					workList["enterprise_size"] = work.get("company_size_from") + "-" + work.get("company_size_to")

			# 添加到简历工作信息列表
			if workList != {"enterprise_name": "", "position_name": "", "experience_desc": "", "start_date": "",
							"end_date": "", "enterprise_size": "", "enterprise_type": "", "work_time": "",
							"enterprise_industry": "", "salary": "", "department": "", "second_job_type": "",
							"first_job_type": ""}:
				resume["workExperienceList"].append(workList)

		# 解析教育信息
		if "education" in d["description"] and isinstance(d["description"]["education"], dict):
			education = d["description"]["education"]
			eduList = {"college_name": "", "profession_name": "", "degree": "", "start_date": "", "end_date": "",
					   "desc": ""}
			if "school" in education:
				college = education.get("school")
				eduList["college_name"] = college
				# 排除爬虫格式错误
				if "<span" in college:
					temp = college.split("<span")
					if len(temp) > 0:
						eduList["college_name"] = temp[0]
				# 学校信息
				if resume["college_name"] == "":
					resume["college_name"] = eduList["college_name"]
			if "speciality" in education:
				eduList["profession_name"] = education.get("speciality")
				if "<td" in eduList["profession_name"]:
					eduList["profession_name"] = ""
					# 错误数据，写入文件
					output.write(out_temp + " error_info: speciality \n")
				if re.match("^\d+$", eduList["profession_name"]):
					eduList["profession_name"] = ""
					output.write(out_temp + " error_info: speciality \n")
				# 专业信息
				if resume["profession_name"] == "":
					resume["profession_name"] = eduList["profession_name"]
			if "stime" in education:
				eduList["start_date"] = education.get("stime").replace(".", "-")
				if eduList["start_date"] == "0":
					eduList["start_date"] = ""
			if "etime" in education:
				eduList["end_date"] = education.get("etime").replace(".", "-")
				if eduList["end_date"] == "0":
					eduList["end_date"] = ""
			if "degree" in education:
				eduList["degree"] = education.get("degree")
				if resume["degree"] == "":
					resume["degree"] = eduList["degree"]
			if "description" in education:
				eduList["desc"] = education.get("description")

			# 添加到教育信息列表
			resume["educationList"].append(eduList)

	# 关闭错误记录文件
	output.close()

	# 返回解析好的简历数据
	return resume
