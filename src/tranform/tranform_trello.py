from datetime import datetime
from src.ingest.crawl_trello import get_information
from src.shared.udf import *

def append_trello_dim_project(dim_project, trello_data_folder_list):
    """
    Append trello data project dimension table
    Input:
        dim_project: blank project dimension dataframe.
        trello_data_folder_list: list of folder contain project (board) data file.
    Output: 
        dim_project: project dimension dataframe after add data.
    """
    dim_project.drop(dim_project.index, inplace=True)
    for trello_data_folder in trello_data_folder_list:
        data_path = trello_data_folder + '/board_information.json'
        data = read_json_file(data_path)
        id = data['id']
        created_date = datetime.fromtimestamp(int(id[0:8],16))
        name = data['name']
        dim_project = dim_project.append({'Id': id, 'Name': name, \
                        'Created_Date': created_date}, ignore_index=True)
    return dim_project

def append_trello_dim_bucket(dim_bucket, trello_data_folder_list):
    """
    Append trello data bucket dimension table
    Input:
        dim_bucket: blank bucket dimension dataframe.
        trello_data_folder_list: list of folder contain bucket data file.
    Output: 
        dim_bucket: bucket dimension dataframe after add data.
    """
    dim_bucket.drop(dim_bucket.index, inplace=True)
    for trello_data_folder in trello_data_folder_list:
        data_path = trello_data_folder + '/bucket_information.json'
        data = read_json_file(data_path)
        for record in data:
            id = record['id']
            name = record['name']
            project_id = record['idBoard']
            dim_bucket = dim_bucket.append({'Id': id, 'Name': name, \
                'Project_Id': project_id}, ignore_index=True)
    return dim_bucket

def append_trello_dim_member(member_id_list, member_refer, dim_member, trello_data_folder_list):
    """
    Append trello data member dimension table
    Input:
        member_id_list: list of member id.
        member_refer: member refer file of HR.
        dim_member: blank member dimension dataframe.
        trello_data_folder_list: list of folder contain member data file.
    Output: 
        dim_member: member dimension dataframe after add data.
    """
    dim_member.drop(dim_member.index, inplace=True)
    for trello_data_folder in trello_data_folder_list:
        data_path = trello_data_folder + '/member_information.json'
        data = read_json_file(data_path)
        for record in data:
            if record['id'] in list(member_refer['Id']):
                if record['id'] not in member_id_list \
                and member_refer.loc[member_refer['Id'] == record['id'],'Account'].values[0] \
                    not in list(dim_member['Account_Id']):    # only add if not exists in table
                    id = record['id']
                    account = member_refer.loc[member_refer['Id'] == id,'Account'].values[0]
                    full_name = record['fullName']
                    user_name = record['username']
                    source_id = 1
                    dim_member = dim_member.append({'Account_Id': account, 'Full_Name': full_name, \
                                'User_Name': user_name, 'Source_Id': source_id}, \
                                ignore_index=True)
                    member_id_list.append(id)
    # dim_member.drop_duplicates(subset=['Account_Id'], keep='first', inplace=True)
    return dim_member, member_id_list

def append_trello_dim_task_allocation(member_refer, dim_task_allocation, trello_data_folder_list):
    """
    Append trello data task allocation dimension table
    Input:
        member_refer: member refer file of HR.
        dim_task_allocation: blank task allocation dimension dataframe.
        trello_data_folder_list: list of folder contain task allocation data file.
    Output: 
        dim_task_allocation: task allocation dimension dataframe after add data.
    """
    dim_task_allocation.drop(dim_task_allocation.index, inplace=True)
    for trello_data_folder in trello_data_folder_list:
        data_path = trello_data_folder + '/card_information.json'
        data = read_json_file(data_path)
        for record in data:
            task_id = record['id']
            if len(record['idMembers']) == 0:
                account_id = None
                dim_task_allocation = dim_task_allocation.append( \
                                        {'Task_Id': task_id, \
                                        'Account_Id': account_id}, \
                                        ignore_index=True)    
            elif len(record['idMembers']) == 1:
                member_id = record['idMembers'][0]
                account_id = member_refer.loc[member_refer['Id'] == member_id, 'Account'].values[0]
                dim_task_allocation = dim_task_allocation.append( \
                                        {'Task_Id': task_id, \
                                        'Account_Id': account_id}, \
                                        ignore_index=True)    
            else:
                for mem_id in record['idMembers']:
                    member_id = mem_id
                    account_id = member_refer.loc[member_refer['Id'] == member_id, 'Account'].values[0]
                    dim_task_allocation = dim_task_allocation.append( \
                                        {'Task_Id': task_id, \
                                        'Account_Id': account_id}, \
                                        ignore_index=True)    
    return dim_task_allocation

def append_trello_fact_task(fact_task, trello_data_folder_list, APIkey, APItoken):
    """
    Append trello data task fact table
    Input:
        fact_task: blank task fact dataframe.
        trello_data_folder_list: list of folder contain task(card) data file.
        APIkey: API key.
        APItoken: API token.
    Output: 
        fact_task: task fact dataframe after add data.
    """
    fact_task.drop(fact_task.index, inplace=True)
    for trello_data_folder in trello_data_folder_list:
        data_path = trello_data_folder + '/card_information.json'
        data = read_json_file(data_path)
        for record in data:
            id = record['id']
            name = record['name']
            description = record['desc']
            created_date = datetime.fromtimestamp(int(id[0:8],16))
            dateLastActivity = record['dateLastActivity']
            start_date = record['start']
            end_date = record['due']  
            bucket_id = record['idList']
            project_id = record['idBoard']
            source_id = 1
            action_info = get_information(f'https://api.trello.com/1/cards/{id}/actions?key={APIkey}&token={APItoken}')
            finished_date = None
            if len(action_info) > 0:
                action_info = action_info[0]
                if 'list' in action_info['data']:
                    if action_info['data']['list']['name'] == 'Done':
                        finished_date = action_info['date']
                elif 'listAfter' in action_info['data']:
                    if action_info['data']['listAfter']['name'] == 'Done':
                        finished_date = action_info['date']
            fact_task = fact_task.append({'Id': id, 'Name': name, \
                        'Description': description, 'Created_Date': created_date, \
                        'Date_Last_Activity': dateLastActivity, \
                        'Start_Date': start_date, 'End_Date': end_date, \
                        'Finished_Date': finished_date, 'Bucket_Id': bucket_id, \
                        'Project_Id': project_id, 'Source_Id': source_id}, \
                        ignore_index=True)
    return fact_task

