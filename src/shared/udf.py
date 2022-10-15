import os
import json
import pandas as pd

def create_storage_folder(folder_path):
    """
    Create data storage folder.
    Input:
        folder_path: path of folder that we need to create.
    Output:
        None
    """
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

def save_json_file(data_path, project_name, file_name, info):
    """
    Save data json file.
    Input:
        data_path: path to save data..
        project_name: name of project.
        file_name: name of file.
        info: data that write to json file.
    Output:
        None
    """
    with open(f'{data_path}/{project_name}/{file_name}', "w", encoding='utf-8') as outfile:
                json.dump(info, outfile, indent=4, ensure_ascii=False)

def get_all_subfolders(root_folder):
    """
    Get all subfolders of specific folder.
    Input:
        root_folder: folder path that need to get subfolders.
    Output: 
        trello_data_folder_list: list of all subfolers.
    """
    trello_data_folder_list = []
    for sub_f in os.listdir(root_folder):
        f_path = os.path.join(root_folder, sub_f)
        if os.path.isdir(f_path):
            trello_data_folder_list.append(f_path)
    return trello_data_folder_list
    
def read_json_file(data_path):
    """
    Read json file.
    Input:
        data_path: path of json file that need read data.
    Output: 
        data: data of json file (dictionary).
    """
    with open(data_path, "r", encoding='utf-8') as f:
        data = json.load(f)
    return data

def append_data_dim_source(dim_source, source_name_list):
    """
    Append data source dimension table
    Input:
        dim_source: blank source dimension dataframe.
        source_name_list: list of source name (Trello, Planner).
    Output:
        dim_source: source dimension dataframe after add data.
    """
    dim_source.drop(dim_source.index, inplace=True)
    dim_source['Name'] = source_name_list
    return dim_source
    
def append_data_dim_date(dim_date, start_date, num_years):
    """
    Append data date dimension table
    Input:
        dim_date: blank date dimension dataframe.
        start_date: the start date (YYYY/MM/dd).
        num_years: number of year that you want to jump for end date.
    Output: 
        dim_date: date dimension dataframe after add data.
    """
    dim_date.drop(dim_date.index, inplace=True)
    start_date = pd.to_datetime(start_date)
    end_date = start_date + pd.DateOffset(years = num_years)
    while start_date < end_date:
        day_ = start_date.strftime('%d')
        day_of_week = start_date.strftime('%w')
        week_ = start_date.strftime('%W')
        month_ = start_date.strftime('%m')
        year_ = start_date.strftime('%Y')
        is_working_day = 'No' if int(day_of_week) in (6, 7) else 'Yes'
        dim_date = dim_date.append({'Date': start_date, 'Day': day_, \
                        'Day_of_week': day_of_week, 'Week': week_, \
                        'Month': month_, 'Year': year_, \
                        'Is_working_day':is_working_day}, ignore_index=True)
        start_date = (start_date + pd.DateOffset(days = 1))
    return dim_date