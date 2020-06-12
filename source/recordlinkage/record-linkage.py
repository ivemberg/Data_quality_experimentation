import recordlinkage
import pandas as pd
from recordlinkage.index import Block
from recordlinkage.preprocessing import clean


data1 = pd.read_csv("data/restaurants/grouped_by_restaurant/ActiveDiner.txt", 
                        encoding = "UTF-8",
                        sep = "\t",
                        names = ["Fonte", "Ristorante", "Indirizzo"] )
df1 = pd.DataFrame(data1)

data2 = pd.read_csv("data/restaurants/grouped_by_restaurant/DiningGuide.txt", 
                        encoding = "UTF-8",
                        sep = "\t",
                        names = ["Fonte", "Ristorante", "Indirizzo"])
df2 = pd.DataFrame(data2)



#recordlinkage.preprocessing.clean(s)
"""
#https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html

indexer = recordlinkage.SortedNeighbourhoodIndex(on = ["Ristorante"])
candidate_links = indexer.index(df1, df2)
print(len(candidate_links))

compare_cl = recordlinkage.Compare()

compare_cl.string('Ristorante', 'Ristorante', threshold=0.85, label='Ristorante')
compare_cl.string('Indirizzo', 'Indirizzo', threshold=0.85, label='Indirizzo')

features = compare_cl.compute(candidate_links, df1, df2)
# Classification step 
matches = features[features.sum(axis=1) > 3] 
print(len(matches))
"""