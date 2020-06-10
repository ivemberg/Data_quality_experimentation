import os

restaurants = ["MenuPages", "NYMag", "NYTimes", "ActiveDiner", "NewYork", "DiningGuide", "OpenTable", "TimeOut", "FoodBuzz", "SavoryCities", "VillageVoice", "TasteSpace"]

for file in os.listdir("./data/restaurants/original"):
    if file.endswith(".txt"):
        for rest in restaurants :
            with open("./data/restaurants/original/"+file, 'r', encoding='windows-1252') as f:
                targets = [line for line in f if rest in line]
                with open("./data/restaurants/"+rest+".txt", 'a', encoding='utf-8') as f_to_w:
                    f_to_w.writelines(targets)