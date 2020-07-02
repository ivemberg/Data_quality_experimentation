import os
import os.path
import glob
import pandas as pd
import re
import shutil
from functools import reduce
import specific_cleaners as sc
import pdb

#restaurants    : [regex, 
#                  join,
#                  column_del,
#                  column_names]

restaurants = {

    'ActiveDiner': [r'\t+|\||,',
                    ['1', '2'],
                    ['4_x', '3_y', '4_y', '5_x', '5_y', '6'],
                    {'1': 'restaurant', '2': 'address', '3_x': 'country'}],
    'DiningGuide': [r'\t+',
                    ['1', '2'],
                    [],
                    {'1': 'restaurant', '2': 'address'}],
    'FoodBuzz': [r'\t+|\||,',
                 ['1', '2'],
                 ['8_x', '9_x', '3_y', '4_y', '5_y', '6_y', '7_y', '8_y', '9_y', '8', '9'],
                 {'1': 'restaurant', '2': 'address', '3_x': 'country', '4_x': 'country_code', '5_x': 'neighborhood', '6_x': 'type', '7_x': 'cost'}],
    'MenuPages': [r'\t+|\||( 0 )',
                  ['2', '4'],
                  ['8_x', '10', '12', '14', '16', '18', '20', '6_y', '5_x', '8_y', '5_y', '8'],
                  {'2': 'restaurant', '4': 'address1', '6_x': 'address2'}],
    'NewYork': [r'\t+|,',
                ['1', '2'],
                ['4_x', '3_y', '4_y'],
                {'1': 'restaurant', '2': 'country', '3_x': 'country_code'}],
    'NYMag': [r'\t+',
              ['1'],
              ['2_y', '2'],
              {'1': 'restaurant', '2_x': 'neighborhood'}],
    'NYTimes': [r'\t+',
                ['1', '2'],
                [],
                {'1': 'restaurant', '2': 'neighborhood'}],
    'OpenTable': [r'\t+',
                  ['1'],
                  ['2_y', '2'],
                  {'1': 'restaurant', '2_x': 'neighborhood'}],
    'SavoryCities': [r'\t+',
                     ['1', '2'],
                     [],
                     {'1': 'restaurant', '2': 'address', '3': 'neighborhood'}],
    'TasteSpace': [r'\t+',
                   ['1', '2'],
                   ['3_y'],
                   {'1': 'restaurant', '2': 'address', '3_x': 'country'}],
    'TimeOut': [r'\t+|,',
                ['1', '2'],
                ['3_y', '4_y', '5_y', '6_y', '7_y', '8_y', '8'],
                {'1': 'restaurant', '2': 'in', '3_x': 'address1', '4_x': 'address2', '5_x': 'address3', '6_x': 'neighborhood', '7_x': 'phone', '8_x': 'type'}],
    'VillageVoice': [r'\t+',
                     ['1'],
                     ['2_y', '3_y', '2', '3'],
                     {'1': 'restaurant', '2_x': 'address', '3_x': 'neighborhood'}]
}


def groupAndSepare(dir, out_dir):
    out_files = set()
    for file in os.listdir(dir):
        if file.endswith('.txt'):
            print(f"\tProcessing {file}...")
            date = file.replace('restaurants', '').replace('.txt', '')
            for rest in restaurants:
                restFile = out_dir + rest + '/' + rest + date + '.csv'
                if restFile not in out_files:
                    out_files.add(restFile)
                with open(dir+file, 'r', encoding='windows-1252') as f:
                    targets = [line.rstrip('\n') for line in f if rest in line]
                    data = pd.DataFrame(
                        [re.split(restaurants[rest][0], record) for record in targets])
                    data.dropna(axis=1, how='all', inplace=True)
                    data.drop(0, axis=1, inplace=True)

                    # Some refactor on 'TimeOut'
                    if 'TimeOut' in rest:
                        if '1_29' in file or '2_05' in file or '2_12' in file or '2_19' in file or '2_16' in file:
                            cols = list(data.columns)
                            # Scambia colonna 3 (cols[2]) con colonna 2 (cols[1])
                            cols[2], cols[1] = cols[1], cols[2]
                            data = data[cols]
                            # Rinomina colonna 3 in 2 e colonna 2 in 3
                            data.rename(columns={2: 3, 3: 2}, inplace=True)

                    data.drop_duplicates(inplace=True)
                    if not (len(data.columns) == 1):
                        data.to_csv(restFile, sep=';')
    return out_files


def mergeDatasets(strDir):
    for rest in restaurants:
        restDfs = []
        print(f"\tProcessing {rest}...")
        files = glob.glob(strDir + rest + '/*.csv')
        for f in files:
            df = pd.read_csv(f, sep=';')
            df.drop('Unnamed: 0', axis=1, inplace=True)  # Cancella colonna 0
            df = df.astype(str)  # Cast a string
            # Sostituzione 'nan' con stringa vuota
            df.replace('nan', '', inplace=True)
            # Applica strip() a tutti i valori
            df = df.apply(lambda x: x.str.strip(), axis=1)
            restDfs.append(df)

        # Elimina la cartella contenente i csv del ristorante
        shutil.rmtree(strDir + rest)

        restDfs_final = reduce(lambda left, right: pd.merge(
            left, right, how='outer', on=restaurants[rest][1]), restDfs)  # Esegue la join su tutti i df del ristorante
        # Sostituzione 'nan' con stringa vuota
        restDfs_final.replace('nan', '', inplace=True)
        # Elimina eventuali colonne duplicate
        restDfs_final = restDfs_final.loc[:,
                                          ~restDfs_final.columns.duplicated()]
        # Elimina eventuali righe duplicate
        restDfs_final.drop_duplicates(inplace=True)
        # Ordina per nome ristorante
        restDfs_final.sort_values(by=restaurants[rest][1][0], inplace=True)
        restDfs_final.to_csv(strDir + rest + '.csv',
                             sep=';', index=False)  # Salva su csv


def cleanDatasets(strDir):
    files = glob.glob(strDir + '*.csv')
    for f in files:
        file_name = os.path.basename(f)
        file_name = os.path.splitext(file_name)[0]

        print(f"\tProcessing {file_name}...")

        df = pd.read_csv(f, sep=';')

        df = df.astype(str)
        if 'FoodBuzz' in file_name:
            df = df.apply(sc.FoodBuzz_cleaner, axis=1)

        if 'MenuPages' in file_name:
            df = df.apply(sc.MenuPages_cleaner, axis=1)

        if 'NYMag' in file_name:
            df = df.apply(sc.NYMag_cleaner, axis=1)

        if 'OpenTable' in file_name:
            df = df.apply(sc.OpenTable_cleaner, axis=1)

        if 'SavoryCities' in file_name:
            df = df.apply(sc.SavoryCities_cleaner, axis=1)
            for i, row in df.iterrows():
                ifor_val = ''
                for k, row_k in df.iterrows():
                    if df.at[i, '1'] == df.at[k, '1']:
                        ifor_val = df.at[k, '2']
                        break
                df.at[i, '2'] = ifor_val

        if 'TimeOut' in file_name:
            df = df.apply(sc.TimeOut_cleaner, axis=1)
            # indexNan = df[df['3_x'] == 'nan'].index
            # df.drop(indexNan, inplace=True)

        if 'VillageVoice' in file_name:
            df = df.apply(sc.VillageVoice_cleaner, axis=1)

        # Remove extra columns
        df = df.drop(restaurants[file_name][2], axis=1)
        # Strip all values
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        df.drop_duplicates(inplace=True)
        df.sort_values(by=restaurants[file_name][1][0], inplace=True)
        df.replace('nan', '', inplace=True)

        df.rename(columns=restaurants[file_name][3], inplace=True)

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

    print("Group and separation phase...")
    groupAndSepare(in_dir, out_dir)
    print("Done!\n")

    print("Merge phase...")
    mergeDatasets(out_dir)
    print("Done!\n")

    print("Clean phase...")
    cleanDatasets(out_dir)
    print("Done!\n")


if __name__ == '__main__':
    main()
