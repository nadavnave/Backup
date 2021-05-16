from os.path import isfile, join
import re
import os
import shutil
import pandas as pd
import argparse
import datetime
import ftplib
import json
import logging
from datetime import date
from tqdm import tqdm

parser = argparse.ArgumentParser(description="backup phone images and videos from WhatsApp and DCIM")

parser.add_argument('ftp_ip',type=str,help='The IP of the ftp server on the Android phone')
parser.add_argument('ftp_port',type=int, help='The port of the ftp server on the Android phone')
parser.add_argument('user',type=str, help='The user to connect to on the ftp server')
parser.add_argument('password',type=str, help='The password to connect to on the ftp server')

logging.basicConfig(filename='backup.log',level=logging.DEBUG)


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
    

def create_ftp_database(ftp, path):
    df = pd.DataFrame(columns=["year","month","day","fullname"])
    files = ftp_dir_files(ftp, path)

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
 

def delete(ftp, origin_path, keep_month):
    df = create_ftp_database(ftp, origin_path)
    cur_month = date.today().month
    cur_year = date.today().year
    df_files_to_delete = df[ ~((df['month'] >= (cur_month - keep_month)) & (df['year'] == cur_year))]
    files_to_delete = df_files_to_delete['fullname'].tolist()
    for file in tqdm(files_to_delete):
        ftp.delete(join(origin_path,file))
        logging.debug("deleted {}".format(file))


def backup(ftp, origin_path, target_path):

    origin_files = set(ftp_dir_files(ftp, origin_path))
    target_files = set([f for f in os.listdir(target_path) if isfile(join(target_path,f))])
    
    not_backed_up = origin_files.difference(target_files)
    
    ftp.cwd(origin_path)
    for file in tqdm(not_backed_up):
        with open(join(target_path,file),"wb") as fp:
            ftp.retrbinary('RETR {}'.format(file), fp.write)

  
def main():
    args = parser.parse_args()

    ip = args.ftp_ip
    port = args.ftp_port
    user = args.user 
    passwd = args.password

    logfile = open("log.txt","w")

    ftp = ftplib.FTP()
    res = ftp.connect(host=ip,port=port)

    res = ftp.login(user=user, passwd=passwd)

    
    with open('conf.json','r') as f:
        config = json.load(f)

    for obj in config:
        create_ftp_database(ftp, obj[0])
        backup(ftp, obj[0], obj[1])
#        delete(ftp, obj[0], obj[2])



if __name__ == "__main__":
    main()

