import requests
import os
from src.shared.udf import *

feature_list = ['', '/lists', '/cards', '/members']
file_name_list = ['board_information.json', 'bucket_information.json', \
                'card_information.json', 'member_information.json']

def get_shortLink(my_user_api_url):
    """
    Get all of my project shortLinks.
    Input:
        my_user_api_url: my user api url.
    Output:
        project_shortLink_list: list of short link projects.
    """
    my_request = requests.get(my_user_api_url)
    projects = my_request.json()
    project_shortLink_list = [project['shortLink'] for project in projects]
    return project_shortLink_list

def get_information(api_url):
    """
    Get information of api_url: board, bucket, label, card, member.
    Input:
        api_url: api url.
    Output:
        info: information of api_url: board, bucket, label, card, member.
    """
    my_request = requests.get(api_url)
    info = my_request.json()
    return info

def get_all_trello_data(my_user_api_url, APIkey, APItoken, trello_data_path):
    """
    Get all trello data and save to json file.
    Input:
        my_user_api_url: my user api url.
        APIkey: API key.
        APItoken: API token.
        trello_data_path: path of data folder of trello.
    """
    # Get shortLink of all projects of my account
    project_shortLink_list = get_shortLink(my_user_api_url)
    api_url_arr = [[f'https://api.trello.com/1/boards/{project_shortLink}{feature}?key={APIkey}&token={APItoken}' \
                        for feature in feature_list] \
                            for project_shortLink in project_shortLink_list]

    print("Starting create data file")
    for api_url_list in api_url_arr:
        # Get information list from api_url_list
        info_list = [get_information(api_url) for api_url in api_url_list]
        project_name = info_list[0]['name'].strip().replace(' ','_')
        
        create_storage_folder(os.path.join(trello_data_path, project_name))
        # f'{trello_data_path}/{project_name}'
        # Save information as json file
        for i in range(len(info_list)):
            save_json_file(trello_data_path, project_name, file_name_list[i], info_list[i])





