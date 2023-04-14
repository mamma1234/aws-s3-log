
import os
import re
import json
import os
import boto3
# from botocore.exceptions import ClientError
from dotenv import load_dotenv
from datetime import datetime
import sys, os

load_dotenv()

dir_path = '/Users/parkdaekyu/Downloads/logs/laravel_logs'
output_dir = '/Users/parkdaekyu/Downloads/logs/join'


def sort_files_by_date(files):
    return sorted(files, key=lambda f: datetime.strptime(f, '%Y-%m-%d.sql'))

def merge_files_by_month(files, output_dir):
    try:
        month_files = {}
        for file in files:
            date_str = file[:-4]
            month = date_str[:7]
            if month not in month_files:
                month_files[month] = []
            month_files[month].append(file)

        # print('-----------------------')
        # print(month_files)
        
        for month, month_files_list in month_files.items():
            # print(month)
            pattern = re.compile(r'^\d{4}-\d{2}$')
            if (not bool(pattern.match(month))):
                continue
            print('month-----------------------', month)
            output_file = os.path.join(output_dir, f'{month}.sql')
            with open(output_file, 'w') as out_file:
                sorted_files = sort_files_by_date(month_files_list)
                for file in sorted_files:
                    with open(dir_path+'/'+file, 'r') as in_file:
                        out_file.write(in_file.read().replace('logispot_develop.log_location_accesses', 'log_location_accesses2'))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print("The error is: ",e)
    
if __name__ == '__main__':
    try:
        print("start process")

        files = os.listdir(dir_path)
            # print(f)
        print(files)
        
        
        # files = [for f in os.listdir(dir_path) if ]
        # files = [dir_path for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        
        
        merge_files_by_month(files, output_dir)

        # load()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        