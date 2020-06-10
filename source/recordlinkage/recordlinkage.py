import recordlinkage
import pandas as pd

data1 = pd.read_csv("C:/Users/strav/git/Data_quality_experimentation/data/restaurants/grouped_by_restaurant/ActiveDiner.txt", sep = "\t")
data1.columns = ["Fonte", "Ristorante", "Indirizzo"]
df1 = pd.DataFrame(data1)

data2 = pd.read_csv("C:/Users/strav/git/Data_quality_experimentation/data/restaurants/grouped_by_restaurant/DiningGuide.txt", sep = "\t")
data2.columns = ["Fonte", "Ristorante", "Indirizzo"]
df2 = pd.DataFrame(data2)

indexer = recordlinkage.Index()
indexer.full()
pairs = indexer.index(df1, df2)

