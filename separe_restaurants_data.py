import os

restaurants = ["MenuPages", "NYMag", "NYTimes", "ActiveDiner", "NewYork", "DiningGuide", "OpenTable", "TimeOut", "FoodBuzz", "SavoryCities", "VillageVoice", "TasteSpace"]

for file in os.listdir("./data/restaurants/original"):
    if file.endswith(".txt"):
        for rest in restaurants :
            with open("./data/restaurants/original/"+file, 'r', encoding='utf-8', errors='ignore') as f:
                targets = [line for line in f if rest in line]
                for record in targets:
                    print(record, file=open("./data/restaurants/"+rest+".txt", "a"), end='', flush=True)