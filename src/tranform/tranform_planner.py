from datetime import datetime
from src.shared.udf import *
import os

def append_planner_dim_project(dim_project, planner_project_data_folder):
    """
    Append planner data project dimension table
    Input:
        dim_project: project dimension dataframe.
        planner_project_data_folder: folder contain planner project data file.
    Output: 
        dim_project: project dimension dataframe after add data.
    """
    for planner_data_file in os.listdir(planner_project_data_folder):
        data = read_json_file(os.path.join(planner_project_data_folder, planner_data_file))
        project_infor = data['plans_list']['value'][0]
        id = project_infor['id']
        created_date = project_infor['createdDateTime']
        name = project_infor['title']
        dim_project = dim_project.append({'Id': id, 'Name': name, \
                        'Created_Date': created_date}, ignore_index=True)
    return dim_project

def append_planner_dim_bucket(dim_bucket, planner_project_data_folder):
    """
    Append planner data bucket dimension table
    Input:
        dim_bucket: bucket dimension dataframe.
        planner_project_data_folder: folder contain planner project data file.
    Output: 
        dim_bucket: bucket dimension dataframe after add data.
    """
    for planner_data_file in os.listdir(planner_project_data_folder):
        data = read_json_file(os.path.join(planner_project_data_folder, planner_data_file))['buckets_list']['value']
        for record in data:
            id = record['id']
            name = record['name']
            project_id = record['planId']
            dim_bucket = dim_bucket.append({'Id': id, 'Name': name, \
                'Project_Id': project_id}, ignore_index=True)
    return dim_bucket

def append_planner_dim_member(member_id_list, dim_member, planner_member_data_folder):
    """
    Append planner data member dimension table
    Input:
        member_id_list: list of member id.
        dim_member: member dimension dataframe.
        planner_member_data_folder: folder contain planner member data file.
    Output: 
        dim_member: member dimension dataframe after add data.
    """
    for planner_data_file in os.listdir(planner_member_data_folder):
        data = read_json_file(os.path.join(planner_member_data_folder, planner_data_file))['Item2']
        for record in data:
            if record['GivenName'].lower() not in [acc_name.lower() \
            for acc_name in dim_member['Account_Id']]:    # only add if not exists in table
                id = record['Id']
                account = record['GivenName']
                full_name = record['DisplayName']
                user_name = record['GivenName']
                source_id = 2
                dim_member = dim_member.append({'Account_Id': account, 'Full_Name': full_name, \
                            'User_Name': user_name, 'Source_Id': source_id}, \
                            ignore_index=True)
                member_id_list.append(id)
    # dim_member.drop_duplicates(subset=['Account_Id'], keep='first', inplace=True)
    return dim_member, member_id_list

def append_planner_dim_task_allocation(member_refer, dim_task_allocation, planner_project_data_folder):
    """
    Append planner data task allocation dimension table
    Input:
        member_refer: member refer file of HR.
        dim_task_allocation: task allocation dimension dataframe.
        planner_project_data_folder: folder contain planner project data file.
    Output: 
        dim_task_allocation: task allocation dimension dataframe after add data.
    """
    for planner_data_file in os.listdir(planner_project_data_folder):
        data = read_json_file(os.path.join(planner_project_data_folder, planner_data_file))['tasks_list']['value']
        for record in data:
            task_id = record['id']
            if len(record['_assignments']) == 0:
                account_id = None
                dim_task_allocation = dim_task_allocation.append( \
                                        {'Task_Id': task_id, \
                                        'Account_Id': account_id}, \
                                        ignore_index=True)    
            elif len(record['_assignments']) == 1:
                id = record['_assignments'][0]['userId']
                if id in list(member_refer['Id']):
                    account_id = member_refer.loc[member_refer['Id'] == id,'Account'].values[0]
                    dim_task_allocation = dim_task_allocation.append( \
                                            {'Task_Id': task_id, \
                                            'Account_Id': account_id}, \
                                            ignore_index=True)    
            else:
                for acc_id in record['_assignments']:
                    id = acc_id['userId']
                    if id in list(member_refer['Id']):
                        account_id = member_refer.loc[member_refer['Id'] == id,'Account'].values[0]
                        dim_task_allocation = dim_task_allocation.append( \
                                            {'Task_Id': task_id, \
                                            'Account_Id': account_id}, \
                                            ignore_index=True)    
    return dim_task_allocation

def append_planner_fact_task(fact_task, planner_project_data_folder):
    """
    Append planner data task fact table
    Input:
        fact_task: task fact dataframe.
        planner_data_folder_list: folder contain task(card) data file.
    Output: 
        fact_task: task fact dataframe after add data.
    """
    for planner_data_file in os.listdir(planner_project_data_folder):
        data = read_json_file(os.path.join(planner_project_data_folder, planner_data_file))
        task_list = data['tasks_list']['value']
        task_detail_list = data['list_task_detail']
        for i in range(len(task_list)):
            id = task_list[i]['id']
            name = task_list[i]['title']
            description = task_detail_list[i]['description']
            created_date = task_list[i]['createdDateTime']
            dateLastActivity = None
            start_date = None
            end_date = None
            finished_date = None
            if 'startDateTime' in task_list[i]:
                start_date = task_list[i]['startDateTime']                
            if 'dueDateTime' in task_list[i]:
                end_date = task_list[i]['dueDateTime']                
            bucket_id = task_list[i]['bucketId']
            project_id = task_list[i]['planId']
            source_id = 2
            if 'completedDateTime' in task_list[i]:
                finished_date = task_list[i]['completedDateTime']
                                
            fact_task = fact_task.append({'Id': id, 'Name': name, \
                        'Description': description, 'Created_Date': created_date, \
                        'Date_Last_Activity': dateLastActivity, \
                        'Start_Date': start_date, 'End_Date': end_date, \
                        'Finished_Date': finished_date, 'Bucket_Id': bucket_id, \
                        'Project_Id': project_id, 'Source_Id': source_id}, \
                        ignore_index=True)
    return fact_task
