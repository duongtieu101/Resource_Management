from src.shared.udf import *
from .tranform_trello import *
from .tranform_planner import *


source_name_list = ['Trello', 'Planner']
def create_dataframe():
    dim_source = pd.DataFrame(columns=['Name'])
    dim_date = pd.DataFrame(columns=['Date', 'Day', 'Day_of_week', \
                                    'Week', 'Month', 'Year', \
                                    'Is_working_day'])
    dim_project = pd.DataFrame(columns=['Id', 'Name', 'Created_Date'])
    dim_bucket = pd.DataFrame(columns=['Id', 'Name', 'Project_Id'])
    dim_member = pd.DataFrame(columns=['Account_Id', 'Full_Name', \
                                        'User_Name', 'Source_Id'])
    dim_task_allocation = pd.DataFrame(columns=['Task_Id', 'Account_Id'])
    fact_task = pd.DataFrame(columns=['Id', 'Name', 'Description', \
                                'Created_Date', 'Date_Last_Activity', 'Start_Date', \
                                'End_Date', 'Finished_Date', 'Bucket_Id', \
                                'Project_Id', 'Source_Id'])
    return dim_source, dim_date, dim_project, dim_bucket, dim_member, dim_task_allocation, fact_task
def tranform_all(APIkey, APItoken, \
                trello_data_path, project_planner_data_path, \
                planner_member_data_folder, member_refer):
    """
    """
    member_id_list = []
    dim_source, dim_date, dim_project, dim_bucket, dim_member, dim_task_allocation, fact_task = create_dataframe()
    print("Starting process data file")
    trello_data_folder_list = get_all_subfolders(trello_data_path)
    print("Starting create data frame")
    dim_source = append_data_dim_source(dim_source, source_name_list)
    dim_date = append_data_dim_date(dim_date, '2021/1/1', 2)
    print("Starting add trello data to data frame")
    dim_project = append_trello_dim_project(dim_project, trello_data_folder_list)
    dim_bucket = append_trello_dim_bucket(dim_bucket, trello_data_folder_list)
    dim_member, member_id_list = append_trello_dim_member(member_id_list, member_refer, dim_member, \
                                    trello_data_folder_list)
    dim_task_allocation = append_trello_dim_task_allocation(member_refer, \
                                                dim_task_allocation, \
                                                trello_data_folder_list)
    fact_task = append_trello_fact_task(fact_task, trello_data_folder_list, APIkey, APItoken)  
    print("Starting add planner data to data frame")
    dim_project = append_planner_dim_project(dim_project, project_planner_data_path)
    dim_bucket = append_planner_dim_bucket(dim_bucket, project_planner_data_path)
    dim_member, member_id_list = append_planner_dim_member(member_id_list, dim_member, \
                                    planner_member_data_folder)
    dim_task_allocation = append_planner_dim_task_allocation(member_refer, \
                            dim_task_allocation, project_planner_data_path)
    fact_task = append_planner_fact_task(fact_task, project_planner_data_path)  

    tables_list = [dim_source, dim_date, dim_project, \
                    dim_bucket, dim_member, dim_task_allocation, \
                    fact_task]
    return tables_list