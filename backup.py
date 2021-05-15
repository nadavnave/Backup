import os
from os.path import isfile, join
import shutil
from sys import argv
import pandas as pd
from datetime import date
from tqdm import tqdm

def delete(origin_path, keep_month):
    df = create_database(origin_path)
    cur_month = date.today().month
    cur_year = date.today().year
    df_files_to_delete = df[ ~((df['month'] >= (cur_month - keep_month)) &
    (df['year'] == cur_year))]
    files_to_delete = database_to_filenames(df_files_to_delete)
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
    df = pd.DataFrame(columns=["type","year","month","day","number","ext"])
    files = [f for f in os.listdir(path) if isfile(join(path,f))]


    for file in files:
        filedict = {}

        filename, extention = file.split(sep=".", maxsplit=2)
        Type, Date,Number = filename.split(sep="-",maxsplit=3)

        filedict['ext'] = extention
        filedict['number'] = Number
        filedict['day'] = int(Date[-2:])
        filedict['month'] = int(Date[4:-2])
        filedict['year'] = int(Date[:4])
        filedict['type'] = Type

        df = df.append(filedict, ignore_index=True)

    df.to_csv("{}.csv".format(path))
    return df
   
   
def main():
    origin_path= argv[1] 
    target_path= argv[2] 
    number_month_keep = int(argv[3])
    backup(origin_path, target_path)
    #delete(origin_path,number_month_keep)


if __name__ == "__main__":
    main()
