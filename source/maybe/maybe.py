import pandas as pd
import numpy as np
from pyjarowinkler import distance
import recordlinkage
import glob
import os

# window aumenta l'ampiezza dell'indexing sortedneighbourhood (= 1 è come usare blocking)
# threshold è la soglia di accettazione compare.string (0<th<=1)
def dedupeDS(window: int, th: float) -> dict:
    datasets = get_datasets("./data/restaurants/gbr_splitted_google/*.csv")
    for rest, ds in datasets.items():
        columns_to_check = ['restaurant']
        if 'addressGoogle' in ds.columns:
            columns_to_check.append('addressGoogle')
        if 'neighborhood' in ds.columns:
            columns_to_check.append('neighborhood')

        indexer = recordlinkage.Index()
        for col in columns_to_check:
            indexer.sortedneighbourhood(left_on=col, window=window)
        candidate_links = indexer.index(ds)

        # recordlinkage.write_annotation_file(f"{rest}.json", candidate_links[0:100], ds, dataset_a_name=rest)

        compare_cl = recordlinkage.Compare(n_jobs=-1)
        for col in columns_to_check:
            compare_cl.string(col, col, threshold=th, label=col)

        features = compare_cl.compute(candidate_links, ds)
        matches = features[features.sum(axis=1) > len(columns_to_check)-1]
        index_to_drop = set(matches.index.get_level_values(1))
        print(f"{rest} matches: {len(matches)}")
        print(f"{index_to_drop}\n")
        ds.drop(index_to_drop, inplace=True)
        datasets[rest] = ds.copy()
    
    return datasets


def get_datasets(from_regx: str) -> dict:
    datasets = dict()

    for file in glob.glob(from_regx):
        df = pd.read_csv(file, sep=';')

        # Drop non-Google obtained address and record without Google address
        droplist = [
            i for i in df.columns if 'address' in i and not 'Google' in i]
        df.drop(droplist, axis=1, inplace=True)
        if 'addressGoogle' in df.columns:
            df.drop(df[df.addressGoogle == ''].index, inplace=True)
        df.drop_duplicates(inplace=True)
        datasets[os.path.basename(file)[:-4]] = df

    return datasets


def link_reduce(from_rest: str, dfs: dict, window: int, th: float, classifier: str, thFusion:float) -> dict:
    dfs_copy = {from_rest: dfs[from_rest]}
    dfs_reduce = dfs.copy()

    # Make copy of dfs with from_rest moved on top
    for rest, df in dfs.items():
        if rest == from_rest:
            continue
        else:
            dfs_copy[rest] = df.copy()

    for rest, df in dfs_copy.items():
        for rr, ddf in dfs_reduce.items():
            if rr == rest:
                continue
            else:
                columns_to_check = ['restaurant']
                print(f"{rest} -> {rr}")
                if df['addressGoogle'].isnull().sum() != len(df['addressGoogle']) and ddf['addressGoogle'].isnull().sum() != len(ddf['addressGoogle']):
                    columns_to_check.append('addressGoogle')
                if df['neighborhood'].isnull().sum() != len(df['neighborhood']) and ddf['neighborhood'].isnull().sum() != len(ddf['neighborhood']):
                    columns_to_check.append('neighborhood')
                
                #print(f"\tcheck: {columns_to_check}")
                indexer = recordlinkage.Index()

                # 1 - INDEXING
                for col in columns_to_check:
                    indexer.sortedneighbourhood(
                        left_on=col, right_on=col, window=window)
                candidate_links = indexer.index(df, ddf)

                # 2 - COMPARISON
                compare_cl = recordlinkage.Compare(n_jobs=-1)
                for col in columns_to_check:
                    if col == 'addressGoogle':
                        compare_cl.exact(col, col)
                    else:
                        compare_cl.string(col, col, label=col, threshold=th, method='jarowinkler')
                features = compare_cl.compute(candidate_links, df, ddf)

                # 3 - CLASSIFICATION
                matches = None
                matches = features[features.sum(axis=1) > len(columns_to_check)-1]
                index_to_drop = set(matches.index.get_level_values(1))
                # if classifier == "ecm":
                #     ecm = recordlinkage.ECMClassifier(
                #         init='jaro', binarize=None, max_iter=100, atol=0.0001, use_col_names=True)
                #     ecm.fit_predict(features)
                #     matches = ecm.predict(features)
                # elif classifier == "kmeans":
                #     kmeans = recordlinkage.KMeansClassifier()
                #     kmeans.fit_predict(features)
                #     matches = kmeans.predict(features)
                
                #print(f"\tmatches: {len(matches)}")
                # 4 - COMBINE INFORMATION
                for left, right in matches.index:
                    combine(df.loc[left], ddf.loc[right], thFusion)
                dfs_copy[rest] = df.copy()

                # 4 - DROP RIGHT ON MATCHES INDEX
                #index_to_drop = set(matches.get_level_values(1))
                #print(f"\tddf before drop: {len(ddf.index)}")
                ddf.drop(index_to_drop, inplace=True)
                dfs_copy[rr] = ddf.copy()
                dfs_reduce[rr] = ddf.copy()
                #print(f"\tddf after drop: {len(dfs_reduce[rr].index)}\n")

        del dfs_reduce[rest]
    
    final_df = pd.concat(list(dfs_copy.values()))
    final_df.dropna(subset=['addressGoogle'], inplace=True)
    final_df.drop_duplicates(inplace=True)
    return final_df


def combine(left:pd.Series, right:pd.Series, thFusion:float):
    # New information arrives from right Series, includes 'addressGoogle','neighborhood','type','cost','phone'   
    if right['type']:
        if not left['type']:
            left['type'] = right['type']
        else:
            lft = str(left['type'])
            lefts = lft.split('/')
            bInsert = False
            for l in lefts:
                rgt = str(right['type'])
                dist = distance.get_jaro_distance(l, rgt, winkler=True, scaling=0.1)
                if dist < thFusion:
                    bInsert = True
                    break
            if bInsert:
                left['type'] = f"{left['type']} / {right['type']}" 

    if right['phone']:
        if not left['phone']:
            left['phone'] = right['phone']

    if right['cost']:
        if not left['cost']:
            left['cost'] = right['cost']

    if right['addressGoogle']:
        if not left['addressGoogle']:
            left['addressGoogle'] = right['addressGoogle']

    if right['neighborhood']:
        if not left['neighborhood']:
            left['neighborhood'] = right['neighborhood']


def align_schema(dicIn: dict):
    columns = ['restaurant','addressGoogle','neighborhood','country','country_code','type','cost','phone']
    for rest, df in dicIn.items():
        for column in columns:
            if not column in df.columns:
                if column == 'country':
                    df[column] = 'New York'
                elif column == 'country_code':
                    df[column] = 'NY'
                else:
                    df[column] = None
        df = df[columns]
        dicIn[rest] = df.copy()


def main():
    data = dedupeDS(9, 0.9)
    align_schema(data)
    reduced = link_reduce('VillageVoice', data, 9, 0.9, "ecm", 0.8)
    reduced.to_csv('test.csv', sep=';', index=None)


if __name__ == '__main__':
    main()
