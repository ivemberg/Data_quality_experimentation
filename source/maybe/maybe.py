import pandas as pd
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


def link_reduce(from_rest: str, dfs: dict, window: int, th: float, classifier: str) -> dict:
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
                if 'addressGoogle' in df.columns and 'addressGoogle' in ddf.columns:
                    columns_to_check.append('addressGoogle')
                if 'neighborhood' in df.columns and 'neighborhood' in ddf.columns:
                    columns_to_check.append('neighborhood')
                indexer = recordlinkage.Index()

                # 1 - INDEXING
                for col in columns_to_check:
                    indexer.sortedneighbourhood(
                        left_on=col, right_on=col, window=window)
                candidate_links = indexer.index(df, ddf)

                # 2 - COMPARISON
                compare_cl = recordlinkage.Compare(n_jobs=-1)
                for col in columns_to_check:
                    compare_cl.string(col, col, threshold=th, label=col)
                features = compare_cl.compute(candidate_links, df, ddf)

                # 3 - CLASSIFICATION
                matches = None
                if classifier == "ecm":
                    ecm = recordlinkage.ECMClassifier(
                        init='jaro', binarize=None, max_iter=100, atol=0.0001, use_col_names=True)
                    ecm.fit_predict(features)
                    matches = ecm.predict(features)
                elif classifier == "kmeans":
                    kmeans = recordlinkage.KMeansClassifier()
                    kmeans.fit_predict(features)
                    matches = kmeans.predict(features)
                
                # 4 - DROP (per adesso)
                index_to_drop = set(matches.get_level_values(1))
                ddf.drop(index_to_drop, inplace=True)
                dfs_copy[rr] = ddf.copy()
                dfs_reduce[rr] = ddf.copy()

        del dfs_reduce[rest]
    
    return dfs_copy


def main():
    data = dedupeDS(9, 0.9)
    reduced = link_reduce('VillageVoice', data, 9, 0.9, "kmeans")
    print(reduced)


if __name__ == '__main__':
    main()
