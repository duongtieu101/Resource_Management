import unittest
from src.tranform.tranform_all_data import tranform_all
import pandas as pd
import os
from dotenv import load_dotenv
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
from pathlib import Path

root_path = Path(__file__).parent.parent
load_dotenv()

APIkey = os.getenv('APIkey')
APItoken = os.getenv('APItoken')
trello_data_path =  os.path.join(root_path, 'raw_data/trello')
project_planner_data_path = os.path.join(root_path, 'raw_data/planner/project')
planner_member_data_folder = os.path.join(root_path, 'raw_data/planner/member')
member_refer = pd.read_excel(os.path.join(root_path, 'raw_data/member_refer.xlsx'), engine='openpyxl')

class TestTranform(unittest.TestCase):
    def setUp(self):
        self.dim_source = pd.DataFrame(columns=['Name'])
        self.dim_date = pd.DataFrame(columns=['Date', 'Day', 'Day_of_week', \
                                        'Week', 'Month', 'Year', \
                                        'Is_working_day'])
        self.dim_project = pd.DataFrame(columns=['Id', 'Name', 'Created_Date'])
        self.dim_bucket = pd.DataFrame(columns=['Id', 'Name', 'Project_Id'])
        self.dim_member = pd.DataFrame(columns=['Account_Id', 'Full_Name', \
                                            'User_Name', 'Source_Id'])
        self.dim_task_allocation = pd.DataFrame(columns=['Task_Id', 'Account_Id'])
        self.fact_task = pd.DataFrame(columns=['Id', 'Name', 'Description', \
                                    'Created_Date', 'Date_Last_Activity', 'Start_Date', \
                                    'End_Date', 'Finished_Date', 'Bucket_Id', \
                                    'Project_Id', 'Source_Id'])
        self.df_list_expected = [self.dim_source, self.dim_date, self.dim_project, \
                        self.dim_bucket, self.dim_member, \
                        self.dim_task_allocation, self.fact_task]

        warnings.simplefilter(action='ignore', category=FutureWarning)
        self.df_list_acctually = tranform_all(APIkey, APItoken, \
                            trello_data_path, project_planner_data_path, \
                            planner_member_data_folder, member_refer)

    def test_tranform_data_len_success(self):
        self.assertTrue(len(self.df_list_acctually) == len(self.df_list_expected))

    def test_tranform_schema(self):
        for i in range(len(self.df_list_acctually)):
            self.assertTrue(set(self.df_list_acctually[i].columns) == set(self.df_list_expected[i].columns))
