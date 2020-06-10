import recordlinkage
import pandas as pd


data1 = pd.read_csv("C:/Users/strav/git/Data_quality_experimentation/data/restaurants/grouped_by_restaurant/ActiveDiner.txt", sep = "\t")
data1.columns = ["Fonte", "Ristorante", "Indirizzo"]
df1 = pd.DataFrame(data1)

data2 = pd.read_csv("C:/Users/strav/git/Data_quality_experimentation/data/restaurants/grouped_by_restaurant/DiningGuide.txt", sep = "\t")
data2.columns = ["Fonte", "Ristorante", "Indirizzo"]
df2 = pd.DataFrame(data2)

#https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html
indexer = recordlinkage.Index()
indexer.full()
pairs = indexer.index(df1, df2)
# print (len(pairs)) 5026779


compare_cl = recordlinkage.Compare()

compare_cl.string('Ristorante', 'Ristorante', method='jarowinkler', threshold=0.85, label='surname')
compare_cl.string('Indirizzo', 'Indirizzo', threshold=0.85, label='address_1')

features = compare_cl.compute(pairs, df1, df2)
print(features)