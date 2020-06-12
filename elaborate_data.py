import os
import os.path
import glob
import pandas as pd
import re
import shutil
from functools import reduce
import specific_cleaners as sc
import pdb

restaurants = {
    #Restaurant     : [regex            , join,         column_del]
    'ActiveDiner'   : [r'\t+|\||,'      , ['1', '2'],   ['4_x', '3_y', '4_y', '5_x', '5_y', '6']], 
    'DiningGuide'   : [r'\t+'           , ['1', '2'],   []],
    'FoodBuzz'      : [r'\t+|\||,'      , ['1', '2'],   ['8_x', '9_x', '3_y', '4_y', '5_y', '6_y', '7_y', '8_y', '9_y', '8', '9']], 
    'MenuPages'     : [r'\t+|\||( 0 )'  , ['2', '4'],   ['8_x', '10', '12', '14', '16', '18', '20', '6_y', '5_x', '8_y', '5_y', '8']], 
    'NewYork'       : [r'\t+|,'         , ['1', '2'],   ['4_x', '3_y', '4_y']], 
    'NYMag'         : [r'\t+'           , ['1'],        ['2_y', '2']], 
    'NYTimes'       : [r'\t+'           , ['1', '2'],   []],     
    'OpenTable'     : [r'\t+'           , ['1'],        []], 
    'SavoryCities'  : [r'\t+'           , ['1', '2'],   []], 
    'TasteSpace'    : [r'\t+'           , ['1', '2'],   []],
    'TimeOut'       : [r'\t+|,'         , ['1', '2'],   []], 
    'VillageVoice'  : [r'\t+'           , ['1'],        []] 
}


def groupAndSepare(dir, out_dir):
    out_files = set()
    for file in os.listdir(dir):
        if file.endswith('.txt'):
            date = file.replace('restaurants', '').replace('.txt', '')
            for rest in restaurants:
                restFile = out_dir + rest + '/' + rest + date + '.csv'
                if restFile not in out_files:
                    out_files.add(restFile)
                with open(dir+file, 'r', encoding='windows-1252') as f:
                    targets = [line.rstrip('\n') for line in f if rest in line]
                    data = pd.DataFrame([re.split(restaurants[rest][0], record) for record in targets])
                    data.dropna(axis=1, how='all', inplace=True)
                    data.drop(0, axis=1, inplace=True)
                    data.drop_duplicates(inplace=True)
                    if not (len(data.columns) == 1):
                        data.to_csv(restFile, sep=';')
    return out_files


def mergeDatasets(strDir):
    for rest in restaurants:
        restDfs = []
        files = glob.glob(strDir + rest + '/*.csv')
        for f in files:
            df = pd.read_csv(f, sep=';')
            df.drop('Unnamed: 0', axis=1, inplace=True)
            restDfs.append(df)        
        
        shutil.rmtree(strDir + rest)

        restDfs_final = reduce(lambda left,right: pd.merge(left,right,how='outer',on=restaurants[rest][1]), restDfs)
        restDfs_final = restDfs_final.loc[:,~restDfs_final.columns.duplicated()]
        restDfs_final.drop_duplicates(inplace=True)
        restDfs_final.sort_values(by=restaurants[rest][1][0], inplace=True)
        restDfs_final.to_csv(strDir + rest + '.csv', sep=';', index=False)


def cleanDatasets(strDir):
    files = glob.glob(strDir + '*.csv')
    for f in files:
        file_name = os.path.basename(f)
        file_name = os.path.splitext(file_name)[0]
        df = pd.read_csv(f, sep=';')

        df = df.astype(str)
        if 'FoodBuzz' in file_name:
            df = df.apply(sc.FoodBuzz_cleaner, axis=1)

        if 'MenuPages' in file_name:
            df = df.apply(sc.MenuPages_cleaner, axis=1)

        if 'NYMag' in file_name:
            df = df.apply(sc.NYMag_cleaner, axis=1)

        #Remove extra columns
        df = df.drop(restaurants[file_name][2], axis=1)
        #Strip all values
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        df.drop_duplicates(inplace=True)
        df.sort_values(by=restaurants[file_name][1][0], inplace=True) 

        os.remove(f)
        df.to_csv(f, sep=';', index=False)

def main():
    in_dir = './data/restaurants/original/'
    out_dir = './data/restaurants/gbr_splitted'

    shutil.rmtree(out_dir)   
    os.mkdir(out_dir)

    out_dir = './data/restaurants/gbr_splitted/'
    for rest in restaurants:
        os.mkdir(out_dir + rest)

    groupAndSepare(in_dir, out_dir)
    mergeDatasets(out_dir)
    cleanDatasets(out_dir)


if __name__ == '__main__':
	main()
