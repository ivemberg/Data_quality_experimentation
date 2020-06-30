import pandas as pd
import recordlinkage

fb = pd.read_csv("data/restaurants/gbr_splitted/FoodBuzz.csv", sep = ";")
fb_cols = ['restaurant', 'neighborhood', 'address', 'country', 'country_code', 'type', 'cost']
fb = fb[fb_cols]

nym = pd.read_csv("data/restaurants/gbr_splitted/NYMag.csv", sep = ";")
nym.rename(columns={"address": "neighborhood"}, inplace = True)

nyt = pd.read_csv("data/restaurants/gbr_splitted/NYMag.csv", sep = ";")
nyt.rename(columns={"address": "neighborhood"}, inplace = True)

ot = pd.read_csv("data/restaurants/gbr_splitted/OpenTable.csv", sep = ";")

sc = pd.read_csv("data/restaurants/gbr_splitted/SavoryCities.csv", sep = ";")
sc_cols = ['restaurant', 'neighborhood', 'address']
sc = sc[sc_cols]


df = fb.append([nym,nyt,ot,sc])

df.drop_duplicates(subset =["restaurant", "neighborhood"], 
                     keep = "first", inplace = True) 

lenght = df.shape[0]
new_index = []

for i in range(lenght):
    new_index.append(i)

df.index = new_index

# deduplication
indexer = recordlinkage.Index()
indexer.sortedneighbourhood(left_on=['restaurant'])
candidate_links = indexer.index(df)

compare_cl = recordlinkage.Compare()
compare_cl.string('restaurant', 'restaurant', threshold=0.85, label='ristorante')
compare_cl.string('neighborhood', 'neighborhood', threshold=0.85, label='quartiere')

features = compare_cl.compute(candidate_links, df)
print(features)

matches = features[features.sum(axis=1) > 1]
print(len(matches))

potential_matches = features[features.sum(axis=1) > 1].reset_index()
potential_matches['Score'] = potential_matches.loc[:, ['ristorante','quartiere']].sum(axis=1)

account_merge = potential_matches.merge(df, left_on="level_0", right_index=True) #, how='outer'
final_merge = account_merge.merge(df, left_on="level_1", right_index=True)
final_merge.dropna(axis='columns', inplace=True, how = 'all') #rimuovo solo colonne tutte NaN
final_merge.drop(columns = ['ristorante', 'quartiere', 'Score'], inplace=True) # so che sono 1 1 2

for i, row in final_merge.iterrows():
    print(row)


#final_merge.to_csv("prova_deduplication.csv", header=True, sep=";", decimal=',', float_format='%.3f', index=False)