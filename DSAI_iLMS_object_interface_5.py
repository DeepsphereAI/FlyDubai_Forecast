from DSAI_iLMS_config_3 import modeule_code,unit_results
from DSAI_iLMS_service_4 import json_response
from DSAI_iLMS_db_connection_2 import connect
import json
import datetime as dt
from datetime import datetime,date,timedelta
from DSAI_iLMS_config_3 import course_detail,unit_map_course,map_user_assessments,quizz_assignments,map_unit_user
import pandas as pd
# from DSAI_iLMS_data_load_6 import delete_user_log
conn = connect()
no_response = 'no response'
vAR_major_pool = {'Ar' : 'Artificial', 'I' : 'Intelligence','D' : 'Data','S' : 'Science','M' : 'Machine',
'L' : 'Learning','E' : 'Exploratory','D' : 'Data','A' : 'Analysis','Ap' : 'Applied','AI':'AI','Eng':'Engineering','a':'artificial'}
vAR_is_demo = "No"
vAR_major_key = vAR_major_pool.keys()
vAR_grade = 'Grade'
vAR_default_grade = 'Professional'
vAR_sub_group_dict = {}

def no_response_fn(res):
    for vAR_value in res:
        if res[vAR_value] == '' or res[vAR_value] == None:
            res[vAR_value] = no_response

def date_extraction(res_date):
    vAR_msecs = res_date.partition('(')[2] 
    vAR_msecs = vAR_msecs.rpartition('-')[0]
    date = dt.datetime.fromtimestamp(int(vAR_msecs)/1e3)
    date = date.strftime("%Y-%m-%d %H:%M:%S:%f")
    return date

def user_full(response,dbuser):
    vAR_resp = []
    time = datetime.now()
    for res in response:
        for vAR_value in res:
            if res[vAR_value] == '' or res[vAR_value] == None:
                res[vAR_value] = no_response

        vAR_data = {}

        vAR_data["user_id"] = res['Id']
        vAR_data["username"] = res['UserName']
        vAR_data["is_demo"] = vAR_is_demo
        if res['ProfileType'] == "Internal":
            vAR_data["is_demo"] = "yes"
        else:
            vAR_data["is_demo"] = "no"
        vAR_data['created_by'] = dbuser
        vAR_data['created_datetime'] = time
        vAR_data['updated_by'] = dbuser
        vAR_data['updated_datetime'] = time
        vAR_resp.append(vAR_data)
    return vAR_resp

def res_user_full(response,dbuser):
    vAR_resp = []
    time = datetime.now()
    for res in response:
        for vAR_value in res:
            if res[vAR_value] == '' or res[vAR_value] == None:
                res[vAR_value] = no_response

        vAR_data = {}

        vAR_data["user_id"] = res['Id']
        vAR_data["username"] = res['UserName']
        vAR_data["user_firstname"] = res['FirstName']
        vAR_data["user_lastname"] = res['LastName']
        vAR_data["user_fullname"] = res['FullName']
        vAR_data['user_access_level'] = res['AccessLevel']
        vAR_data["user_brand"] = res['Brand']
        vAR_data['user_email'] = res['Email']
        vAR_data['user_active_status'] = res['Active']
        vAR_data['user_adrs_street1'] = res['Street1']
        vAR_data['user_adrs_street2'] = res['Street2']
        vAR_data['user_city'] = res['City']
        vAR_data['user_state'] = res['State']
        vAR_data['user_postalcode'] = res['PostalCode']
        vAR_data['user_country'] = res['Country']
        vAR_data["teacher_name"] = res['CustomField1']
        vAR_data["parent_name"] = res['CustomField2']
        vAR_data["current_grade"] = res['CustomField3']
        vAR_data["school"] = res['CustomField4']
        vAR_data["batch"] = res['CustomField5']
        vAR_data["team"] = res['CustomField6']
        vAR_data["user_level"] = res['CustomField7'].lower()
        vAR_data['user_company_name'] = res['CompanyName']
        vAR_data["is_demo"] = vAR_is_demo
        if res['ProfileType'] == "Internal":
            vAR_data["is_demo"] = "yes"
        else:
            vAR_data["is_demo"] = "no"
        vAR_data["teacher_id"] = res['CustomField8']
        vAR_data["student_external_id"] = res['CustomField9']
        vAR_data["parent_mobile"] = res['CustomField10']
        vAR_data["user_last_login"] = res['LastLogin']
        vAR_data['user_original_id'] = res['OriginalId']
        vAR_data['created_by'] = dbuser
        # date_extraction(res['CreatedDate'])
        vAR_data['created_datetime'] = time
        vAR_data['updated_by'] = dbuser
        # date_extraction(res['CreatedDate'])
        vAR_data['updated_datetime'] = time
        #vAR_data[''] = res['']
        

        vAR_resp.append(vAR_data)
    return vAR_resp


def resp_user(response):
    vAR_resp = []
    for res in response:
        for vAR_value in res:
            if res[vAR_value] == '' or res[vAR_value] == None:
                res[vAR_value] = no_response

        vAR_data = {}

        vAR_data["user_id"] = res['Id']
        vAR_data["username"] = res['FullName']
        vAR_data["is_demo"] = vAR_is_demo

        vAR_resp.append(vAR_data)
    return vAR_resp

def resp_userdetail(res):
    vAR_data = {}

    for vAR_vals in res:
        if res[vAR_vals] == '' or res[vAR_vals] == None:
            res[vAR_vals] = no_response
    # vAR_data['index'] = 0
    vAR_data["user_id"] = res['Id']
    vAR_data["username"] = res['UserName']
    vAR_data["user_firstname"] = res['FirstName']
    vAR_data["user_lastname"] = res['LastName']
    vAR_data["user_fullname"] = res['FullName']
    vAR_data['user_accesslevel'] = res['AccessLevel']
    vAR_data["user_brand"] = res['Brand']
    vAR_data['user_email'] = res['Email']
    vAR_data['user_active_status'] = res['Active']
    vAR_data['user_adrs_street1'] = res['Street1']
    vAR_data['user_adrs_street2'] = res['Street2']
    vAR_data['user_city'] = res['City']
    vAR_data['user_state'] = res['State']
    vAR_data['user_postalcode'] = res['PostalCode']
    vAR_data['user_country'] = res['Country']
    #vAR_data['user_original_id'] = res['OriginalId']
    vAR_data["teacher_name"] = res['CustomField1']
    vAR_data["parent_name"] = res['CustomField2']
    vAR_data["current_grade"] = res['CustomField3']
    vAR_data["school"] = res['CustomField4']
    vAR_data["batch"] = res['CustomField5']
    vAR_data["team"] = res['CustomField6']
    vAR_data["user_level"] = res['CustomField7'].lower()
    vAR_data['user_company_name'] = res['CompanyName']
    vAR_data["is_demo"] = vAR_is_demo
    vAR_data["teacher_id"] = res['CustomField8']
    vAR_data["student_id"] = res['CustomField9']
    vAR_data["parent_mobile"] = res['CustomField10']
    vAR_data["user_last_login"] = res['LastLogin']
    vAR_data['user_original_id'] = res['OriginalId']

    #vAR_data[''] = res['']
    

    return vAR_data

def student_obi(vAR_res,dbuser):
    vAR_datas = []
    time = datetime.now()
    for res in vAR_res:
        if res['CustomField7'].lower() == "learner":
            vAR_data = {}
            no_response_fn(res)
            # vAR_data['index'] = 0
            vAR_data["student_id"] = res['Id']
            vAR_data["user_id"] = res['Id']
            vAR_data["student_name"] = res['FullName']
            vAR_data["student_grade"] = res['CustomField3']
            vAR_data['student_original_id'] = res['OriginalId']
            vAR_data["student_external_id"] = res['CustomField9']
            vAR_data["teacher_id"] = res['Id']
            vAR_data["parent_id"] = res['Id']
            vAR_data["parent_name"] = res['CustomField2']
            vAR_data["parent_contact"] = res['CustomField10']
            vAR_data['parent_email'] = no_response
            vAR_data['parent_guardian'] = no_response
            if res['ProfileType'] == "Internal":
                vAR_data["is_demo"] = "yes"
            else:
                vAR_data["is_demo"] = "no"
            vAR_data['created_by'] = dbuser
            vAR_data['created_datetime'] = time
            vAR_data['updated_by'] = dbuser
            vAR_data['updated_datetime'] = time
            vAR_datas.append(vAR_data)
    return vAR_datas

def teacher_obi(vAR_res,dbuser):
    vAR_datas = []
    time = datetime.now()
    for res in vAR_res:
        if res['CustomField7'].lower() == "teacher":
            vAR_data = {}
            no_response_fn(res)
            # vAR_data['index'] = 0
            vAR_data["teacher_id"] = res['Id']
            vAR_data["student_id"] = res['Id']
            vAR_data["teacher_name"] = res['FullName']
            vAR_data['teacher_status'] = res['Active']
            if res['ProfileType'] == "Internal":
                vAR_data["is_demo"] = "yes"
            else:
                vAR_data["is_demo"] = "no"
            vAR_data['created_by'] = dbuser
            vAR_data['created_datetime'] = time
            vAR_data['updated_by'] = dbuser
            vAR_data['updated_datetime'] = time
            vAR_datas.append(vAR_data)
    return vAR_datas

def learning_path_obi(rspns,dbuser):
    vAR_course_data = []
    time = datetime.now()
    for resp in rspns:
        vAR_course = {}
        vAR_course["learning_path_id"] = resp['Id']
        vAR_course["learning_path_name"] = resp['Name']
        vAR_course['learning_path_original_id'] = resp['OriginalId']
        vAR_course["is_demo"] = vAR_is_demo
        vAR_course["created_by"] = dbuser
        vAR_course["created_datetime"] = time
        vAR_course["updated_by"] = dbuser
        vAR_course["updated_datetime"] = time
        no_response_fn(vAR_course)
        vAR_course_data.append(vAR_course)
    return vAR_course_data


def resp_course(rspns,dbuser):

    vAR_course_data = []
    time = datetime.now()
    for resp in rspns:
        for vAR_value in resp:
            if resp[vAR_value] == '' or resp[vAR_value] == None:
                resp[vAR_value] = no_response

        vAR_course = {}
        b=""
        v=""
        w=""

        vAR_course["learning_path_id"] = resp['Id']
        vAR_course["learning_path_Name"] = resp['Name']
        vAR_name_split = vAR_course["learning_path_Name"].split()
        for i in range(len(vAR_name_split)):
            for k in vAR_major_key:
                if vAR_name_split[i] == vAR_major_pool[k]:
                    v = vAR_major_pool[k]
                    b = b + " " + v
        vAR_course["learning_path_major"] = b
        vAR_course["learning_path_status"] = no_response
        if vAR_course["learning_path_major"] == "":
            vAR_course["learning_path_major"] = no_response
        for j in range(len(vAR_name_split)):
            if vAR_name_split[j] == vAR_grade:
                w = vAR_name_split[j-1]
                vAR_course["learning_path_status"] = vAR_grade
            if vAR_name_split[j] == "Semester":
                w = vAR_name_split[j+1]
                vAR_course["learning_path_status"] = "Semester"
        if ("level" in vAR_course["learning_path_Name"].lower()) and ("schools" in vAR_course["learning_path_Name"].lower()):
            vAR_course["learning_path_status"] = "Level"
        vAR_course["learning_path_grade"] = w
        if vAR_course["learning_path_grade"] == "":
            vAR_course["learning_path_grade"] = vAR_default_grade
        vAR_course["learning_path_Active_status"] = resp['Active']
        vAR_course["learning_path_Original_id"] = resp['OriginalId']
        vAR_course["learning_path_Description"] = resp["Description"]
        vAR_course['is_demo'] = 'no response'
        vAR_course["created_by"] = dbuser
        vAR_course["created_datetime"] = time
        vAR_course["updated_by"] = dbuser
        vAR_course["updated_datetime"] = time
        vAR_course["source"] = 'Learning path'
        vAR_course_data.append(vAR_course)

    return vAR_course_data

def resp_course_users(vAR_course_ids,dbuser):

    vAR_users_course1 = []
    vAR_cr_ids = ['M9KaginLd1A1','dm3vL8-DWvY1']
    for Cr_Ids in vAR_course_ids:
        vAR_idx = Cr_Ids[0]
        if vAR_idx in vAR_cr_ids:
            # print(vAR_idx)
            # print("cr_user Working?")
            vAR_url = map_unit_user(vAR_idx)
            respn = json_response(vAR_url)
            # print(len(respn))
            time = datetime.now()
            for users in respn:
                for vAR_vals in users:
                    if users[vAR_vals] == '' or users[vAR_vals] == None:
                        users[vAR_vals] = no_response
                vAR_user_course = {}

                vAR_user_course["user_Id"] = users['Id']
                vAR_user_course["learning_path_completion_status"] = users['Completed']
                vAR_user_course["learning_path_completion_percent"] = users['PercentageComplete']
                vAR_user_course['learning_path_id'] = vAR_idx
                vAR_user_course['created_by'] = dbuser
                vAR_user_course['created_datetime'] = time
                vAR_user_course['updated_by'] = dbuser
                vAR_user_course['updated_datetime'] = time
                # vAR_user_course["enroll_date"] = no_response
                vAR_user_course["is_demo"] = vAR_is_demo

                vAR_users_course1.append(vAR_user_course)
        else:
            # print("lpuser")
            vAR_url = course_detail(vAR_idx)
            respn = json_response(vAR_url)
            time = datetime.now()
            for users in respn:
                for vAR_vals in users:
                    if users[vAR_vals] == '' or users[vAR_vals] == None:
                        users[vAR_vals] = no_response
                vAR_user_course = {}

                vAR_user_course["user_Id"] = users['Id']
                vAR_user_course["learning_path_completion_status"] = users['Completed']
                vAR_user_course["learning_path_completion_percent"] = users['PercentageComplete']
                vAR_user_course['learning_path_id'] = vAR_idx
                vAR_user_course['created_by'] = dbuser
                vAR_user_course['created_datetime'] = time
                vAR_user_course['updated_by'] = dbuser
                vAR_user_course['updated_datetime'] = time
                # vAR_user_course["enroll_date"] = no_response
                vAR_user_course["is_demo"] = vAR_is_demo

                vAR_users_course1.append(vAR_user_course)

    return vAR_users_course1

def course_obi(vAR_course_ids,dbuser):
    vAR_lp_units = []
    time = datetime.now()
    vAR_cr_ids = ['M9KaginLd1A1','dm3vL8-DWvY1']
    for vAR_indx in vAR_course_ids:
        if vAR_indx[0] in vAR_cr_ids:
            vAR_url = modeule_code(vAR_indx[0])
            vAR_modules = json_response(vAR_url)
            for vAR_module in vAR_modules:
                vAR_units = {}
                vAR_units['course_id'] = vAR_module['Id']
                vAR_units['course_name'] = vAR_module['Name']
                vAR_units['course_original_id'] = 'no_response' #vAR_module['OriginalId']
                vAR_units['is_demo'] = 'No'
                vAR_units['created_by'] = dbuser
                vAR_units['created_datetime'] = time
                vAR_units['updated_by'] = dbuser
                vAR_units['updated_datetime'] = time
                vAR_lp_units.append(vAR_units)
                for vAR_keys in vAR_module:
                    if vAR_module[vAR_keys] == ''or vAR_module[vAR_keys] == None:
                        vAR_module[vAR_keys] = no_response
        else:
            vAR_url = unit_map_course(vAR_indx[0])
            vAR_unit_data = json_response(vAR_url)
            for vAR_unit in vAR_unit_data:
                vAR_units = {}
                vAR_units['course_id'] = vAR_unit['Id']
                vAR_units['course_name'] = vAR_unit['Name']
                vAR_units['course_original_id'] = vAR_unit['OriginalId']
                vAR_units['is_demo'] = 'No'
                vAR_units['created_by'] = dbuser
                vAR_units['created_datetime'] = time
                vAR_units['updated_by'] = dbuser
                vAR_units['updated_datetime'] = time
                vAR_lp_units.append(vAR_units)
                for vAR_keys in vAR_unit:
                    if vAR_unit[vAR_keys] == ''or vAR_unit[vAR_keys] == None:
                        vAR_unit[vAR_keys] = no_response

    return vAR_lp_units

def course_details_obi(vAR_course_ids,dbuser):
    vAR_crs_dets = []
    time = datetime.now()
    vAR_cr_ids = ['M9KaginLd1A1','dm3vL8-DWvY1']
    for vAR_indx in vAR_course_ids:
        if vAR_indx[0] in vAR_cr_ids:
            vAR_url = modeule_code(vAR_indx[0])
            vAR_modules = json_response(vAR_url)
            for vAR_module in vAR_modules:
                vAR_units = {}
                vAR_units['learning_path_id'] = vAR_indx[0]
                vAR_units['created_by'] = dbuser
                vAR_units['created_datetime'] = time
                vAR_units['updated_by'] = dbuser
                vAR_units['updated_datetime'] = time
                vAR_units['course_id'] = vAR_module["Id"]
                vAR_units['course_name'] = vAR_module["Name"]
                vAR_units['course_original_id'] = vAR_module["Code"]#vAR_module['OriginalId']
                vAR_units['course_description'] = vAR_module['Description']
                vAR_units['course_status'] = vAR_module['Active']
                vAR_units["course_sub_group"] = no_response
                vAR_units['course_code'] = vAR_module["Code"]
                vAR_units['is_demo'] = 'No'
                vAR_units['source'] = 'Module'
                vAR_crs_dets.append(vAR_units)
        else:
            vAR_url = unit_map_course(vAR_indx[0])
            vAR_unit_data = json_response(vAR_url)
            for vAR_unit in vAR_unit_data:
                vAR_units = {}
                vAR_units['learning_path_id'] = vAR_indx[0]
                vAR_units['created_by'] = dbuser
                vAR_units['created_datetime'] = time
                vAR_units['updated_by'] = dbuser
                vAR_units['updated_datetime'] = time
                vAR_units['course_id'] = vAR_unit["Id"]
                vAR_units['course_name'] = vAR_unit["Name"]
                vAR_units['course_original_id'] = vAR_unit['OriginalId']
                vAR_units['course_description'] = vAR_unit['Description']
                vAR_units['course_status'] = vAR_unit['Active']
                vAR_units["course_sub_group"] = no_response
                vAR_units['course_code'] = vAR_unit["Code"]
                vAR_units['is_demo'] = 'No'
                vAR_units['source'] = 'Course'
                vAR_crs_dets.append(vAR_units)
    return vAR_crs_dets

def course_units(vAR_units):
    vAR_mod_unit = []
    for vAR_unit in vAR_units:
        for vAR_keys in vAR_unit:
            if vAR_unit[vAR_keys] == ''or vAR_unit[vAR_keys] == None:
                vAR_unit[vAR_keys] = no_response
        vAR_unit_dict = {}

        vAR_unit_dict["unit_id"] = vAR_unit["Id"]
        vAR_unit_dict["unit_code"] = vAR_unit["Code"]
        vAR_unit_dict["unit_name"] = vAR_unit["Name"]
        vAR_unit_dict["unit_active_status"] = vAR_unit["Active"]
        vAR_unit_dict["unit_description"] = vAR_unit["Description"]
        vAR_unit_dict["unit_type"] = vAR_unit["ModuleType"]
        vAR_unit_dict["unit_duration"] = no_response
        vAR_unit_dict["unit_completion_status"] = no_response

        vAR_mod_unit.append(vAR_unit_dict)

    return vAR_mod_unit

def units_data_intf(rspns):

    vAR_unit_data = []

    for resp in rspns:
        for vAR_value in resp:
            if resp[vAR_value] == '' or resp[vAR_value] == None:
                resp[vAR_value] = no_response

        vAR_unit = {}

        vAR_unit["course_id"] = resp['Id']
        vAR_unit["course_original_id"] = resp['OriginalId']
        vAR_unit["course_code"] = resp['Code']
        vAR_unit["course_Name"] = resp['Name']
        vAR_unit["active_status"] = resp['Active']
        vAR_unit["course_description"] = resp["Description"]

        vAR_unit_data.append(vAR_unit)

    return vAR_unit_data

def unit_course_intf(vAR_course_ids,dbuser):
    # print(len(vAR_course_ids))
    vAR_unit_course = []
    time = datetime.now()
    # vAR_cr_ids = ['M9KaginLd1A1','dm3vL8-DWvY1']
    count = 0
    for vAR_indx in vAR_course_ids:
        # if vAR_indx[0] in vAR_cr_ids:
        #     pass
        # else:
            # start = datetime.now()
            vAR_url = unit_map_course(vAR_indx[0])
            rspns = json_response(vAR_url)
            print(len(rspns))
            count += len(rspns)
            if (rspns != []) or (rspns != {}):
                for vAR_resp in rspns:
                    if 'unit' in vAR_resp["Name"].lower():
                        for vAR_vals in vAR_resp:
                            if vAR_resp[vAR_vals] == '' or vAR_resp[vAR_vals] == None:
                                vAR_resp[vAR_vals] = no_response

                        vAR_unit_cr = {}
                        vAR_unit_cr["unit_id"] = vAR_resp["Id"]
                        vAR_unit_cr["assesment_code"] = 'no_response'
                        # vAR_url = modeule_code(vAR_unit_cr["unit_id"])
                        # vAR_data = json_response(vAR_url)
                        # for vAR_dat in vAR_data:
                        #     if (vAR_dat != []) & (vAR_dat != 'Detail'):
                        #         if vAR_dat['Name'] == 'AI Driven Personalized Assesments':
                        #             vAR_unit_cr["assesment_code"] = vAR_dat['Code']
                        vAR_unit_cr["unit_name"] = vAR_resp["Name"]
                        vAR_unit_cr["unit_code"] = vAR_resp["Code"]
                        vAR_unit_cr["unit_original_id"] = vAR_resp["OriginalId"]
                        vAR_unit_cr["unit_description"] = vAR_resp["Description"]
                        vAR_unit_cr["unit_active_status"] = vAR_resp["Active"]
                        vAR_unit_cr["sub_group_id"] = no_response
                        vAR_unit_cr['course_id'] = vAR_indx[0]
                        vAR_unit_cr['created_by'] = dbuser
                        vAR_unit_cr['created_datetime'] = time
                        vAR_unit_cr['updated_by'] = dbuser
                        vAR_unit_cr['updated_datetime'] = time
                        vAR_unit_cr['source'] = 'Course'
                        # vAR_unit_cr['is_demo'] = 'No'

                        vAR_unit_course.append(vAR_unit_cr)
            # end = datetime.now()
            # print("ID",vAR_indx[0])
            # print("Time difference", end - start)
            # print('\n')
    print("total count",count)
    return vAR_unit_course

def code_match(code,vAR_part_list,sub_group_id,sub_group_name):
    conn = connect()
    vAR_cur = conn.cursor()
    vAR_cur.execute("SELECT course_id,course_original_id FROM dsai_ilms_unmapped_course WHERE course_original_id = '"+code+"'")
    vAR_course_id = vAR_cur.fetchall()
    v = vAR_course_id[0]
    vAR_url = modeule_code(v[0])
    vAR_data = json_response(vAR_url)
    for vAR_dat in vAR_data:
        #print(vAR_dat['Name'])
        vAR_part_dict = {}
        if vAR_dat != []:
            #print(v[0])
            if 'unit' in vAR_dat['Name'].lower():
                vAR_part_dict["unit_description"] = vAR_dat["Description"]
                vAR_part_dict["unit_active_status"] = vAR_dat["Active"]
                vAR_part_dict["unit_id"] = vAR_dat["Id"]
                vAR_part_dict["unit_name"] = vAR_dat["Name"]
                vAR_part_dict["unit_code"] = vAR_dat["Code"]
                vAR_part_dict["unit_original_id"] = vAR_dat["Code"]
                vAR_part_dict["sub_group_id"] = sub_group_id
                g = vAR_part_dict["unit_code"]
                vAR_cur.execute("SELECT course_id FROM dsai_ilms_unmapped_course WHERE course_original_id = '"+g+"'")
                vAR_course_id = vAR_cur.fetchall()
                v1 = vAR_course_id[0]
                id = v1[0]
                vAR_url1 = modeule_code(id)
                vAR_data1 = json_response(vAR_url1)
                for vAR_dat1 in vAR_data1:
                    if (vAR_dat1 != []) & (vAR_dat1 != 'Detail'):
                        if vAR_dat1['Name'] == 'AI Driven Personalized Assesments':
                            vAR_part_dict["assesment_code"] = vAR_dat1['Code']
                vAR_part_list.append(vAR_part_dict)
            elif ('unit' not in vAR_dat['Name'].lower()) & ('assessments' not in vAR_dat['Name'].lower()):
                sub_group_id += '-'
                sub_group_id += vAR_dat['Code']
                print(sub_group_id)
                sub_group_name += ':'
                sub_group_name += vAR_dat['Name']
                print(sub_group_name)
                if 'assessments' in vAR_dat['Name'].lower():
                    pass
                else:
                    code_match(vAR_dat['Code'],vAR_part_list,sub_group_id,sub_group_name)
            else: pass
    vAR_sub_group_dict[sub_group_id] = sub_group_name
    #print(sub_group_name)


def unit_intf(rspns,conn):
    vAR_cur = conn.cursor()
    vAR_unit_course = []
    for vAR_resp in rspns:
        for vAR_vals in vAR_resp:
            if vAR_resp[vAR_vals] == '' or vAR_resp[vAR_vals] == None:
                vAR_resp[vAR_vals] = no_response
        sub_group_id = ''
        sub_group_name = ''
        if ('Part One' in vAR_resp["Name"]) | ('Part Two' in vAR_resp["Name"]):
            #print(vAR_resp['Name'])
            sub_group_id = vAR_resp["Code"]
            sub_group_name = vAR_resp["Name"]
            sub_group_name = sub_group_name.partition(':')[0]
            vAR_unit_cr = {}
            vAR_list = []
            if vAR_resp["Code"] != no_response:
                #print(vAR_unit_cr["unit_code"])
                #vAR_query = "SELECT course_id FROM dsai_ilms_unmapped_course WHERE course_original_id = '"+f+"'"
                #print(sub_group_id)
                code_match(vAR_resp["Code"],vAR_list,sub_group_id,sub_group_name)
                for vAR_list1 in vAR_list:
                    vAR_unit_course.append(vAR_list1)
        else:
            vAR_unit_cr = {}
            vAR_unit_cr["unit_id"] = vAR_resp["Id"]
            vAR_unit_cr["assesment_code"] = '0'
            vAR_unit_cr["unit_name"] = vAR_resp["Name"]
            #print(vAR_unit_cr)
            vAR_unit_cr["unit_code"] = vAR_resp["Code"]
            vAR_unit_cr["unit_original_id"] = vAR_resp["Code"]
            # f=vAR_resp["Code"]
            # if vAR_unit_cr["unit_code"] != no_response:
            #     #print(vAR_unit_cr["unit_code"])
            #     #vAR_query = "SELECT course_id FROM dsai_ilms_unmapped_course WHERE course_original_id = '"+f+"'"
            #     vAR_cur.execute("SELECT course_id FROM dsai_ilms_unmapped_course WHERE course_original_id = '"+f+"'")
            #     vAR_course_id = vAR_cur.fetchall()
            #     # print(vAR_course_id)
            #     if vAR_course_id != []:
            #         v = vAR_course_id[0]
            #         vAR_url = modeule_code(v[0])
            #         vAR_data = json_response(vAR_url)
            #         for vAR_dat in vAR_data:
            #             if (vAR_dat != []) & (vAR_dat != 'Detail'):
            #                 if vAR_dat['Name'] == 'AI Driven Personalized Assesments':
            #                     vAR_unit_cr["assesment_code"] = vAR_dat['Code']
            vAR_unit_cr["unit_description"] = vAR_resp["Description"]
            vAR_unit_cr["unit_active_status"] = vAR_resp["Active"]
            vAR_unit_cr["sub_group_id"] = no_response
            vAR_unit_course.append(vAR_unit_cr)
    if vAR_sub_group_dict != {}:
        json_dict = json.dumps(vAR_sub_group_dict,indent=2)
        vAR_cur.execute("UPDATE dsai_ilms_course_details SET course_sub_group = '"+json_dict+"' WHERE course_original_id ='6242176'")
        #vAR_cur.execute(vAR_q1,json_dict)
    return vAR_unit_course

def resp_unit_to_users(respn):

    vAR_users_unit1 = []
    for users in respn:
        for vAR_vals in users:
            if users[vAR_vals] == '' or users[vAR_vals] == None:
                users[vAR_vals] = no_response
        vAR_user_unit = {}

        vAR_user_unit["user_id"] = users['Id']
        vAR_user_unit["course_completion_status"] = users['Completed']
        vAR_user_unit["course_completion_percent"] = users['PercentageComplete']
        vAR_user_unit["course_complaint_till"] = users["CompliantTill"]
        vAR_user_unit["course_completion_duedate"] = users["DueDate"]
        #vAR_user_unit["user_access_expiry"] = users["AccessTillDate"]
        # vAR_user_unit["is_demo"] = vAR_is_demo

        vAR_users_unit1.append(vAR_user_unit)


    return vAR_users_unit1

def date_extraction(res_date):
    vAR_msecs = res_date.partition('(')[2] 
    vAR_msecs = vAR_msecs.rpartition('-')[0]
    date = dt.datetime.fromtimestamp(int(vAR_msecs)/1e3)
    return date

def assessment_quiz_intrfc(dbuser):
    vAR_user_asmnts_q = []
    vAR_user_asmnts_a = []
    vAR_cur = conn.cursor()
    vAR_since = pd.to_datetime("2021-1-1")
    vAR_since = pd.to_datetime(vAR_since.strftime('%Y-%m-%d'))
    count = 0
    today = pd.to_datetime(date.today().strftime('%Y-%m-%d'))
    today = today + timedelta(days=1)
    print("starts_obi",datetime.now())
    while vAR_since != today:
        vAR_dif = today - vAR_since 
        if vAR_dif.days < 7:
            vAR_to = today
        else:
            vAR_to = vAR_since + timedelta(days=7)
            vAR_to = pd.to_datetime(vAR_to.strftime('%Y-%m-%d'))
        count += 1
        vAR_since = vAR_since.strftime('%Y-%m-%d')
        vAR_to = vAR_to.strftime('%Y-%m-%d')
        print(vAR_since,vAR_to)
        vAR_url = unit_results(vAR_since,vAR_to)
        vAR_resp = json_response(vAR_url)

        for vAR_user_qa in vAR_resp:
                if 'Quiz' in vAR_user_qa['Name']:
                    vAR_assesment_qa = {}
                    vAR_assesment_qa['quiz_id'] = vAR_user_qa['Id']
                    vAR_assesment_qa['user_id'] = vAR_user_qa['UserId']
                    vAR_assesment_qa['assessment_id'] = ""
                    vAR_assesment_qa['original_mod_id'] = vAR_user_qa['OriginalModuleId']
                    vAR_assesment_qa['quiz_code'] = vAR_user_qa['Code']
                    vAR_assesment_qa['quiz_name'] = vAR_user_qa['Name']
                    vAR_assesment_qa['quiz_score'] = vAR_user_qa['Score']
                    vAR_assesment_qa['assesment_grade'] = no_response
                    vAR_assesment_qa['quiz_attempts'] = vAR_user_qa['AttemptNumber']
                    vAR_assesment_qa['quiz_result_id'] = vAR_user_qa['ModuleResultId']
                    vAR_assesment_qa['is_demo'] = 'No'
                    vAR_assesment_qa["created_by"] = vAR_user_qa["CreatedBy"]
                    vAR_assesment_qa['created_datetime'] = date_extraction(vAR_user_qa["CreatedDate"])
                    vAR_assesment_qa['updated_by'] = vAR_user_qa["CreatedBy"]
                    vAR_assesment_qa['updated_datetime'] = date_extraction(vAR_user_qa["UpdatedDate"])                    

                    vAR_user_asmnts_q.append(vAR_assesment_qa)

                if 'Assignments' in vAR_user_qa['Name']:
                    vAR_assesment_qa = {}
                    vAR_assesment_qa['assignment_id'] = vAR_user_qa['Id']
                    vAR_assesment_qa['user_id'] = vAR_user_qa['UserId']
                    vAR_assesment_qa['assessment_id'] = ""
                    vAR_assesment_qa['original_mod_id'] = vAR_user_qa['OriginalModuleId']
                    vAR_assesment_qa['assignment_code'] = vAR_user_qa['Code']
                    vAR_assesment_qa['assignment_name'] = vAR_user_qa['Name']
                    vAR_assesment_qa['assignment_score'] = vAR_user_qa['Score']
                    vAR_assesment_qa['assesment_grade'] = no_response
                    vAR_assesment_qa['assignment_attempts'] = vAR_user_qa['AttemptNumber']
                    vAR_assesment_qa['assignment_result_id'] = vAR_user_qa['ModuleResultId']
                    vAR_assesment_qa['is_demo'] = 'No'
                    vAR_assesment_qa["created_by"] = vAR_user_qa["CreatedBy"]
                    vAR_assesment_qa['created_datetime'] = date_extraction(vAR_user_qa["CreatedDate"])
                    vAR_assesment_qa['updated_by'] = vAR_user_qa["CreatedBy"]
                    vAR_assesment_qa['updated_datetime'] = date_extraction(vAR_user_qa["UpdatedDate"])

                    vAR_user_asmnts_a.append(vAR_assesment_qa)
                    
        vAR_since = pd.to_datetime(vAR_to)
        if vAR_since == today:
                print("count : ",count)
                print("ends_obi",datetime.now())
                break
    vAR_tuple = (vAR_user_asmnts_q,vAR_user_asmnts_a)
    return vAR_tuple

