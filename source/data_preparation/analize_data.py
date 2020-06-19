import pandas as pd 
import numpy
import glob
import os

path = './data/restaurants/original/'
info = []
restaurants = ['ActiveDiner', 'DiningGuide', 'FoodBuzz', 'MenuPages', 'NewYork', 'NYMag', 'NYTimes', 'OpenTable', 'SavoryCities', 'TasteSpace', 'TimeOut', 'VillageVoice']

for filename in glob.glob(os.path.join(path, '*.txt')):
   with open(filename, 'r') as f: 
      df = pd.DataFrame(f)
      i = filename, df.shape
      info.append(i)

path2 = './data/restaurants/grouped_by_restaurant'
info2 = []

for filename in glob.glob(os.path.join(path2, '*.txt')):
   with open(filename, 'r') as f: 
      df = pd.DataFrame(f)
      i = filename, df.shape
      info2.append(i)

print(info2)
