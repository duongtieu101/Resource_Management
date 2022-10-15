import unittest
import os
from dotenv import load_dotenv
from src.ingest.crawl_trello import get_all_trello_data
from src.shared.udf import get_all_subfolders
import pathlib as pl
import shutil
from pathlib import Path

root_path = Path(__file__).parent.parent
load_dotenv()

my_user_id = os.getenv('my_user_id')
APIkey = os.getenv('APIkey')
APItoken = os.getenv('APItoken')
account_name = os.getenv('account_name')
account_key = os.getenv('account_key')
container_name = os.getenv('container_name')  

data_path =  os.path.join(root_path, 'raw_data/trello')

my_user_api_url = f"https://api.trello.com/1/members/{my_user_id}/boards?key={APIkey}&token={APItoken}"

file_name_list = ['board_information.json', 'bucket_information.json', \
                'card_information.json', 'member_information.json']

class TestFileIngest(unittest.TestCase):
    def assertIsFile(self, path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))

class TestIngestTrello(TestFileIngest):
    def setUp(self):
        if not os.path.exists(data_path):
            os.mkdir(data_path)
    def test_get_trello_data(self):
        get_all_trello_data(my_user_api_url, APIkey, APItoken, data_path)
        trello_data_folder_list = get_all_subfolders(data_path)
        for data_folder in trello_data_folder_list:
            for file_name in file_name_list:
                self.assertIsFile(os.path.join(data_folder, file_name))
    # def tearDown(self):
    #     if os.path.exists(data_path):
    #         shutil.rmtree(data_path, ignore_errors=True)

if __name__ == '__main__':
    unittest.main(verbosity=2)

