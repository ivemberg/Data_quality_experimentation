import recordlinkage
import pandas as pd
import pdb
from recordlinkage.index import Block
from recordlinkage.preprocessing import clean


data1 = pd.read_csv("data/restaurants/gbr_splitted/ActiveDiner.csv", 
                        sep = ";") # 1 2 3_x
                        
df1 = pd.DataFrame(data1)
df1.columns = ['Restaurant_1', 'Address_1', 'Country_1']

data2 = pd.read_csv("data/restaurants/gbr_splitted/DiningGuide.csv",
                        sep = ";")
                        
df2 = pd.DataFrame(data2)
df2.columns = ['Restaurant_2', 'Address_2']

print(df1,df2)


# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html

indexer = recordlinkage.Index()
indexer.sortedneighbourhood(left_on='Restaurant_1', right_on='Restaurant_2')
candidate_links = indexer.index(df1,df2)
print(candidate_links)


comp = recordlinkage.Compare()

comp.string('Restaurant_1', 'Restaurant_2', label='Ristorante')
comp.string('Address_1', 'Address_2', label='Indirizzo')
features = comp.compute(candidate_links, df1, df2)
print(features)
print(features.sum(axis=1).value_counts().sort_index(ascending=False))

# https://pbpython.com/record-linking.html

potential_matches = features[features.sum(axis=1) > 1].reset_index()
potential_matches['Score'] = potential_matches.loc[:, 'Ristorante':'Indirizzo'].sum(axis=1)
print(potential_matches)

df1['ActiveDiner_Lookup'] = df1[['Restaurant_1', 'Address_1', 'Country_1']].apply(lambda x: '_'.join(x), axis=1)

df2['DiningGuide_Lookup'] = df2[['Restaurant_2', 'Address_2']].apply(lambda x: '_'.join(x), axis=1)

df1_lookup = df1[['ActiveDiner_Lookup']].reset_index()
df2_lookup = df2[['DiningGuide_Lookup']].reset_index()

data_merge = potential_matches.merge(df1_lookup, how='left')
final_merge = data_merge.merge(df2_lookup, how='left')

cols = ['ActiveDiner', 'DiningGuide', 'Score',
        'df1_lookup', 'df2_lookup']
final_merge[cols].sort_values(by=['ActiveDiner', 'Score'], ascending=False)

print(final_merge)

