import recordlinkage
import pandas as pd
import pdb
from recordlinkage.index import Block
from recordlinkage.preprocessing import clean
import addressGoogleMaps

# Load data
pdb.set_trace()
#df1 = pd.read_csv("../../data/restaurants/gbr_splitted/ActiveDiner.csv", sep = ";") 
df1 = pd.read_csv("DiningGuide_Fixed.csv", sep = ";")
df1 = df1.add_prefix('0_')
#df1 = df1[0:30]

#df2 = pd.read_csv("../../data/restaurants/gbr_splitted/DiningGuide.csv", sep = ";")
df2 = pd.read_csv("DiningGuide_Fixed.csv", sep = ";")
df2 = df2.add_prefix('1_')
#df2 = df2[0:30]

# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html

indexer = recordlinkage.Index()
# This method is very useful when there are many misspellings in the string were used for indexing
indexer.sortedneighbourhood(left_on='0_addressFix', right_on='1_addressFix')
candidate_links = indexer.index(df1,df2)
for couple in candidate_links:
        ind = couple[0]
        #print(df1.take(ind))

comp = recordlinkage.Compare()

comp.string('0_restaurant', '1_restaurant', label='ristorante')
comp.string('0_addressFix', '1_addressFix', label='indirizzo')
features = comp.compute(candidate_links, df1, df2)
# print(features)
# print(features.sum(axis=1).value_counts().sort_index(ascending=False))

# https://pbpython.com/record-linking.html

potential_matches = features[features.sum(axis=1) > 1].reset_index()
potential_matches['Score'] = potential_matches.loc[:, 'ristorante':'indirizzo'].sum(axis=1)
print(potential_matches)

'''
df1['ActiveDiner_Lookup'] = df1[['0_restaurant', '0_address', '0_country']].apply(lambda x: ''.join(str(x)), axis=1)
df2['DiningGuide_Lookup'] = df2[['1_restaurant', '1_address']].apply(lambda x: ''.join(str(x)), axis=1)

df1_lookup = df1[['ActiveDiner_Lookup']].reset_index()
df2_lookup = df2[['DiningGuide_Lookup']].reset_index()

account_merge = potential_matches.merge(df1_lookup, left_on="level_0", right_index=True, how='left')
final_merge = account_merge.merge(df2_lookup, left_on="level_1", right_index=True, how='left')
final_merge.to_csv('test_OKAMI.csv', header=True, sep=";", decimal=',', float_format='%.3f')
'''

account_merge = potential_matches.merge(df1, left_on="level_0", right_index=True, how='left')
final_merge = account_merge.merge(df2, left_on="level_1", right_index=True, how='left')
final_merge = final_merge.sort_values("Score", ascending=False)
final_merge.to_csv('test_OKAMI.csv', header=True, sep=";", decimal=',', float_format='%.3f')