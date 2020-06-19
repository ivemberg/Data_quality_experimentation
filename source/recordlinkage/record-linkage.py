import recordlinkage
import pandas as pd
import pdb
from recordlinkage.index import Block
from recordlinkage.preprocessing import clean


data1 = pd.read_csv("data/restaurants/gbr_splitted/ActiveDiner.csv", 
                        sep = ";") # 1 2 3_x
df1 = pd.DataFrame(data1)

data2 = pd.read_csv("data/restaurants/gbr_splitted/FoodBuzz.csv",
                        sep = ";")
df2 = pd.DataFrame(data2)

print(df1,df2)

# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html

indexer = recordlinkage.Index()
indexer.block(left_on=['restaurant', 'address'], right_on=['restaurant','address'])
candidate_links = indexer.index(df1,df2)
print(candidate_links)


comp = recordlinkage.Compare()

comp.string('restaurant', 'restaurant',  method='jarowinkler', threshold=0.95, label='Ristorante')
comp.string('address', 'address',  method='jarowinkler', threshold=0.95, label='Indirizzo')
comp.string('country', 'country',   method='jarowinkler', threshold=0.95, label='Citta')

features = comp.compute(candidate_links, df1, df2)
print(features)
#print(features.sum(axis=1).value_counts().sort_index(ascending=False))
#matches = features[features.sum(axis=1) > 3] 
#print(matches)


