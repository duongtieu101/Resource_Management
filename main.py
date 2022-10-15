import os
from dotenv import load_dotenv
from src.ingest.crawl_trello import get_all_trello_data
from src.ingest.crawl_planner import get_all_planner_data
from src.tranform.tranform_all_data import tranform_all
from src.load.load import load
from src.shared.udf import create_storage_folder
import warnings
import pandas as pd
warnings.simplefilter(action='ignore', category=FutureWarning)

cwd = os.path.dirname(os.path.realpath(__file__))
load_dotenv()

my_user_id = os.getenv('my_user_id')
APIkey = os.getenv('APIkey')
APItoken = os.getenv('APItoken')
account_name = os.getenv('account_name')
account_key = os.getenv('account_key')
container_name = os.getenv('container_name')
database_server = os.getenv('server')
password = os.getenv('pwd')

trello_data_path =  os.path.join(cwd, 'raw_data/trello')
project_planner_data_path = os.path.join(cwd, 'raw_data/planner/project')
planner_member_data_folder = os.path.join(cwd, 'raw_data/planner/member')
member_refer = pd.read_excel(os.path.join(cwd, 'raw_data/member_refer.xlsx'), engine='openpyxl')

my_user_api_url = f"https://api.trello.com/1/members/{my_user_id}/boards?key={APIkey}&token={APItoken}"

def main():
    """
        - Create folder to store raw data.
        - Ingest data (trello and planner).
        - Tranform data.
        - Load data to database.
    """
    # cwd = os.path.dirname(os.path.realpath(__file__))
    storage_folder_list = ['raw_data/trello', \
                        'raw_data/planner', 'raw_data/planner/project']

    for storage_folder in storage_folder_list:
        create_storage_folder(os.path.join(cwd, storage_folder))
    
    # Ingest data
    get_all_trello_data(my_user_api_url, APIkey, APItoken, trello_data_path)
    get_all_planner_data(account_name, account_key, container_name, project_planner_data_path)
    
    # tranform
    tables_list = tranform_all(APIkey, APItoken, \
                            trello_data_path, project_planner_data_path, \
                            planner_member_data_folder, member_refer)

    # load
    load(database_server, password, tables_list)

    print("End.")
        
if __name__ == "__main__":
    main()