import recordlinkage
import pandas as pd
import pdb
from recordlinkage.index import Block
from recordlinkage.preprocessing import clean


data1 = pd.read_csv("data/restaurants/gbr_splitted/ActiveDiner.csv", 
                        sep = ";") # 1 2 3_x
df1 = pd.DataFrame(data1)

data2 = pd.read_csv("data/restaurants/gbr_splitted/FoodBuzz.csv",
                        sep = ";") # 1 2 3_x
df2 = pd.DataFrame(data2)

print(df1,df2)

# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html

indexer = recordlinkage.Index()
indexer.block(left_on=['1', '2'], right_on=['1','2'])
candidate_links = indexer.index(df1,df2)
print(len(candidate_links))

comp = recordlinkage.Compare()

comp.string('1', '1', method = 'lcs', label='Ristorante')
comp.string('2', '2', method = 'lcs', label='Indirizzo')
comp.string('3_x', '3_x',  method = 'lcs', label='Citta')

features = comp.compute(candidate_links, df1, df2)
print(features.sum(axis=1).value_counts().sort_index(ascending=False))
#matches = features[features.sum(axis=1) > 3] 
#print(matches)
