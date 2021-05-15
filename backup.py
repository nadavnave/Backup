from os.path import isfile, join
import re
import os
import shutil
import pandas as pd
import argparse
from datetime import date
from tqdm import tqdm

parser = argparse.ArgumentParser(description="backup phone images and videos from WhatsApp and DCIM")

parser.add_argument('origin_path',type=str,help='The folder path to backup')
parser.add_argument('target_path',type=str,help='The folder path to backup to')
parser.add_argument('number_month_keep',type=int,help='The number of month prior to today to keep on the origin folder')

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

def database_to_filenames(df):
    filenames = []
    for i in range(len(df)):
        row = df.loc[i]
        filename = "{}-{}{}{}-{}.{}".format(row['type'],str(row['year']),str(row['month']).zfill(2),str(row['day']).zfill(2),row['number'],row['ext'])
        filenames.append(filename)

    return filenames


def create_database(path):
    df = pd.DataFrame(columns=["year","month","day","fullname"])
    files = [f for f in os.listdir(path) if isfile(join(path,f))]


    for f in files:
        filedict = {}

        gr = re.search('(20\d{2})([01]\d)([0-3]\d)',f).groups()

        if len(gr) != 3:
            print("ERROR")

        filedict['year'] = gr[0]
        filedict['month'] = gr[1]
        filedict['day'] = gr[2]
        filedict['fullname'] = f

        df = df.append(filedict, ignore_index=True)

    df.to_csv("{}.csv".format(path))
    return df
   
   
def main():
    args = parser.parse_args()

    origin_path = args.origin_path
    target_path = args.target_path
    number_month_keep = args.number_month_keep
    create_database(origin_path)
    #backup(origin_path, target_path)

    #delete(origin_path,number_month_keep)


if __name__ == "__main__":
    main()
