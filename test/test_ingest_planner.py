import unittest
import os
from dotenv import load_dotenv
from src.ingest.crawl_planner import get_all_planner_data
from .test_ingest_trello import TestFileIngest
import shutil
import warnings
from pathlib import Path

root_path = Path(__file__).parent.parent
load_dotenv()

account_name = os.getenv('account_name')
account_key = os.getenv('account_key')
container_name = os.getenv('container_name')

data_path =  os.path.join(root_path, 'raw_data/planner/project')

class TestIngestPlanner(TestFileIngest):
    def setUp(self):
        if not os.path.exists(data_path):
            os.mkdir(data_path)
    def test_get_planner_data_success(self):
        if not os.path.exists(data_path):
            os.mkdir(data_path)
        warnings.simplefilter("ignore", ResourceWarning)
        get_all_planner_data(account_name, account_key, container_name, data_path)
        for data_file in os.listdir(data_path):
            self.assertIsFile(os.path.join(data_path, data_file))
    # def tearDown(self):
    #     if os.path.exists(data_path):
    #         shutil.rmtree(data_path, ignore_errors=True)    
        
if __name__ == '__main__':
    unittest.main(verbosity=2)
    

