from os.path import isfile, join
import re
import os
import shutil
import pandas as pd
import argparse
import datetime
import ftplib
import json
from datetime import date
from tqdm import tqdm

parser = argparse.ArgumentParser(description="backup phone images and videos from WhatsApp and DCIM")

parser.add_argument('ftp_ip',type=str,help='The IP of the ftp server on the Android phone')
parser.add_argument('ftp_port',type=int, help='The port of the ftp server on the Android phone')
parser.add_argument('user',type=str, help='The user to connect to on the ftp server')
parser.add_argument('password',type=str, help='The password to connect to on the ftp server')


def ftp_dir_files(ftp,path):
    ftp.cwd(path)
    dir_lines = []
    ftp.retrlines('LIST',dir_lines.append)
    return [dir_line.split(' ')[-1] for dir_line in dir_lines if ftp_is_file(dir_line)]

def ftp_is_file(dir_line):
    permisstions = dir_line.split(' ')[0]
    if permisstions[0] == 'd':
        return False
    return True
    

def create_database(path):
    df = pd.DataFrame(columns=["year","month","day","fullname"])
    files = [f for f in os.listdir(path) if isfile(join(path,f))]

    for f in files:
        filedict = {}

        gr = re.search('(20\d{2})([01]\d)([0-3]\d)',f).groups()

        if len(gr) != 3:
            raise VALUEERROR

        year = filedict['year'] = int(gr[0])
        month = filedict['month'] = int(gr[1])
        day = filedict['day'] = int(gr[2])
        filedict['fullname'] = f

        datetime.datetime(year=year,month=month,day=day)

        df = df.append(filedict, ignore_index=True)

    df.to_csv("{}.csv".format(path.replace('/','_')))
    return df
 

def delete(origin_path, keep_month):
    df = create_database(origin_path)
    cur_month = date.today().month
    cur_year = date.today().year
    df_files_to_delete = df[ ~((df['month'] >= (cur_month - keep_month)) &
    (df['year'] == cur_year))]
    files_to_delete = df_files_to_delete['fullname'].list()
    for file in tqdm(files_to_delete):
        os.remove(join(origin_path,file))
        print("deleted {}".format(file))


def backup(origin_path, target_path):

    origin_files = set([f for f in os.listdir(origin_path) if isfile(join(origin_path,f))])
    target_files = set([f for f in os.listdir(target_path) if
        isfile(join(target_path,f))])
    
    not_backed_up = origin_files.difference(target_files)
    
    for file in tqdm(not_backed_up):
        shutil.copy(join(origin_path,file),join(target_path,file))

  
def main():
    args = parser.parse_args()

    ip = args.ftp_ip
    port = args.port
    user = args.user
    passwd = args.password
    create_database(origin_path)
    with open('conf.json','r') as f:
        config = json.load(f)

    for obj in config:
        create_database(obj[0])

    #backup(origin_path, target_path)
    #delete(origin_path,number_month_keep)


if __name__ == "__main__":
    main()
