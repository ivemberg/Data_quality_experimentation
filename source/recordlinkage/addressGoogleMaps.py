import pdb
import requests
import json
import pprint
import pandas as pd

def googleAddress(s):
	headers = {
		'authority': 'maps.googleapis.com',
		'pragma': 'no-cache',
		'cache-control': 'no-cache',
		'upgrade-insecure-requests': '1',
		'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
		'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
		'x-client-data': 'CIe2yQEIo7bJAQjBtskBCKmdygEI36DKAQj+vMoBCIbHygEYgrjKARibvsoB',
		'sec-fetch-site': 'none',
		'sec-fetch-mode': 'navigate',
		'sec-fetch-user': '?1',
		'sec-fetch-dest': 'document',
		'accept-language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,la;q=0.5,es;q=0.4',
	}

	params = (
		('query', s),
		('key', 'AIzaSyBTbyH43SpUNDtJKkQnGzMt9qF-fs-cdVw'),
		('language', 'en')
	)

	response = requests.get('https://maps.googleapis.com/maps/api/place/textsearch/json', params=params)
	#pdb.set_trace()
	raw_json = json.loads(response.text)
	pprint.pprint(raw_json)
	if len(raw_json["results"])==1 :
		return raw_json["results"][0]["formatted_address"]
	else :
		return s	

if __name__ == "__main__":
	#googleAddress("917-790-2525")


	df1 = pd.read_csv("../../data/restaurants/gbr_splitted/DiningGuide.csv", sep = ";") 

	df1["addressFix"] = df1["address"].apply(googleAddress)

	df1.to_csv('DiningGuide.csv', header=True, sep=";", decimal=',', float_format='%.3f')



'''
# import the library
import googlemaps
import json
import pprint

# Define the API Key.
API_KEY = 'AIzaSyBTbyH43SpUNDtJKkQnGzMt9qF-fs-cdVw'

# Define the Client
gmaps = googlemaps.Client(key = API_KEY)

# Do a simple nearby search where we specify the location
# in lat/lon format, along with a radius measured in meters
places_result  = gmaps.place(input_text="matador 57 Greenwich Avenue")

pprint.pprint(places_result)
'''