import glob
import os
import shutil

class Service():
    def __init__(self):
        self.csv_files = glob.glob(os.path.join(os.getcwd() + '/input-directory', '*.csv'))
        self.processed = glob.glob(os.path.join(os.getcwd() + '/processed', '*.csv'))
        for process in self.processed:
            if process in self.csv_files:
                self.csv_files.remove(process)
        self.col_info = [
            {
                "col_name": "INTERNAL_ID",
                "datatype": "int",
                "nullable": False,
                "condition": {
                    "len": 8
                }
            },
            {
                "col_name": "FIRST_NAME",
                "datatype": "string",
                "nullable": False,
                "condition": {
                    "max_len": 15
                }
            },
            {
                "col_name": "MIDDLE_NAME",
                "datatype": "string",
                "nullable": True,
                "condition": {
                    "max_len": 15
                }
            },
            {
                "col_name": "LAST_NAME",
                "datatype": "string",
                "nullable": False,
                "condition": {
                    "max_len": 15
                }
            },
            {
                "col_name": "PHONE_NUM",
                "datatype": "string",
                "nullable": False,
                "condition": {
                    "max_len": 15
                }
            }
        ]

    def move_file(self, source, destination):
        shutil.move(source, destination)
