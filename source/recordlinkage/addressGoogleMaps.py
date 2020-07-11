import pdb
import requests
import json
import pprint
import pandas as pd

def googleAddress(s):
	# headers = {
	# 	'authority': 'maps.googleapis.com',
	# 	'pragma': 'no-cache',
	# 	'cache-control': 'no-cache',
	# 	'upgrade-insecure-requests': '1',
	# 	'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
	# 	'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	# 	'x-client-data': 'CIe2yQEIo7bJAQjBtskBCKmdygEI36DKAQj+vMoBCIbHygEYgrjKARibvsoB',
	# 	'sec-fetch-site': 'none',
	# 	'sec-fetch-mode': 'navigate',
	# 	'sec-fetch-user': '?1',
	# 	'sec-fetch-dest': 'document',
	# 	'accept-language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,la;q=0.5,es;q=0.4',
	# }

	if not s or s is 'nan' or pd.isna(s) or str(s).isspace():
		return ''
	
	params = (
		('query', str(s) + " Ney York"),
		('key', 'AIzaSyDhW1CY1fl_QZ_TZrS7ssL6VzcZSqShZCM'),
		('language', 'en')
	)

	response = requests.get('https://maps.googleapis.com/maps/api/place/textsearch/json', params=params)
	raw_json = json.loads(response.text)
	pprint.pprint(raw_json)
	if len(raw_json["results"])<=5 and len(raw_json["results"])>0:
		return raw_json["results"][0]["formatted_address"].split(',')[0]
	else :
		return s	

if __name__ == "__main__":

	name = "VillageVoice"
	df1 = pd.read_csv("./data/restaurants/gbr_splitted/" + name + ".csv", sep = ";") 
	df1["addressGoogle"] = df1["address"].apply(googleAddress)

	# MEGLIO LASCIARE IL PATH CORRENTE E POI SPOSTARLO A MANO NELLA CARTELLA gbr_splitted_google
	df1.to_csv(name + '.csv', header=True, sep=";", decimal=',', float_format='%.3f')