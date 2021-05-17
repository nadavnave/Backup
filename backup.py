from os.path import isfile, join
import sys
import re
import os
import shutil
import pandas as pd
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import ftplib
import json
import logging
from datetime import date
from tqdm import tqdm

parser = argparse.ArgumentParser(description="backup phone images and videos from WhatsApp and DCIM")

parser.add_argument('ftp_ip', type=str, help='The IP of the ftp server on the Android phone')
parser.add_argument('ftp_port', type=int, help='The port of the ftp server on the Android phone')
parser.add_argument('user', type=str, help='The user to connect to on the ftp server')
parser.add_argument('password', type=str, help='The password to connect to on the ftp server')

logging.basicConfig(filename='backup.log', level=logging.DEBUG, datefmt="%Y-%m-%d %H:%M:%S", format="%(asctime)s %(levelname)-8s %(message)s")


def ftp_dir_files(ftp, path):
    ftp.cwd(path)
    dir_lines = []
    ftp.retrlines('LIST', dir_lines.append)
    return [dir_line.split(' ')[-1] for dir_line in dir_lines if ftp_is_file(dir_line)]


def ftp_is_file(dir_line):
    permisstions = dir_line.split(' ')[0]
    if permisstions[0] == 'd':
        return False
    return True
    

def create_ftp_database(ftp, path):
    df = pd.DataFrame(columns=["date", "fullname"])
    files = ftp_dir_files(ftp, path)

    for f in tqdm(files,desc="Createing Database for {}".format(path)):
        filedict = {}

        # Extract date from the filename from the format 'YYYYMMDD'
        # anywhere in the filname
        regex_res = re.search('(20\d{2})([01]\d)([0-3]\d)', f)
        if regex_res == None:
            logging.error("Unable to parse file {}".format(f))
            continue


        gr = regex_res.groups()


        year = int(gr[0])
        month = int(gr[1])
        day  = int(gr[2])
        date = datetime.date(year=year, month=month, day=day)
        filedict['date'] = date
        filedict['fullname'] = f


        df = df.append(filedict, ignore_index=True)

    df.to_csv("{}.csv".format(path.replace('/', '_')))
    return df
 

def delete(ftp, origin_path, keep_month):
    df = create_ftp_database(ftp, origin_path)
    cur_date= date.today()

    df_files_to_delete = df[ df['date'] < (cur_date - relativedelta(months=keep_month)) ]
    files_to_delete = df_files_to_delete['fullname'].tolist()

    for file in tqdm(files_to_delete, desc="Deleting {}".format(origin_path)):
        ftp.delete(join(origin_path, file))
        logging.debug("deleted {}".format(file))
        


def backup(ftp, origin_path, target_path):

    origin_files = set(ftp_dir_files(ftp, origin_path))
    target_files = set([f for f in os.listdir(target_path) if isfile(join(target_path, f))])
    
    not_backed_up = origin_files.difference(target_files)
    
    ftp.cwd(origin_path)
    for file in tqdm(not_backed_up, desc="Copying {}".format(origin_path)):
        with open(join(target_path, file), "wb") as fp:
            ftp.retrbinary('RETR {}'.format(file), fp.write)

  
def main():
    args = parser.parse_args()

    ip = args.ftp_ip
    port = args.ftp_port
    user = args.user 
    passwd = args.password

    logfile = open("log.txt", "w")

    ftp = ftplib.FTP()
    res = ftp.connect(host=ip, port=port)

    res = ftp.login(user=user, passwd=passwd)

    
    with open('conf.json', 'r') as f:
        config = json.load(f)

    for obj in config:
        logging.debug("Backuping {}".format(obj[0]))
         
        backup(ftp, obj[0], obj[1])
        
        delete(ftp, obj[0], obj[2])



if __name__ == "__main__":
    main()

