import logging
logging.basicConfig(level=logging.DEBUG)
import fuzzymatcher
import pandas as pd
import pdb

def strip(text):
    try:
        return text.strip()
    except AttributeError:
        return text

def test_fuzzy_left_join():

	pd.set_option('display.max_columns', 4)
	'''
	pd.set_option('display.max_rows', None)
	pd.set_option('display.width', None)
	pd.set_option('display.max_colwidth', None)
	pd.set_option('display.max_rows', None)
	pd.set_option('display.max_columns', None)
	'''

	ons = pd.read_csv("./data/restaurants/grouped_by_restaurant/ActiveDiner.txt",
						sep='\t',
						names=["Fonte", "Ristorante", "Indirizzo"],
						converters = {
							'Fonte' : strip,
							'Ristorante' : strip,
							'Indirizzo' : strip})

	os = pd.read_csv("./data/restaurants/grouped_by_restaurant/DiningGuide.txt",
						sep='\t',
						names=["Fonte2", "Ristorante2", "Indirizzo2"],
						converters = {
							'Fonte2' : strip,
							'Ristorante2' : strip,
							'Indirizzo2' : strip})

	# Columns to match on from df_left
	left_on = ["Ristorante", "Indirizzo"]

	# Columns to match on from df_right
	right_on = ["Ristorante2", "Indirizzo2"]

	df_joined = fuzzymatcher.fuzzy_left_join(ons, os, left_on = left_on, right_on = right_on)
	rename = {"best_match_score": "Score"}
	df_joined = df_joined.rename(columns=rename)
	df_joined = df_joined.sort_values("Score", ascending=False)
	df_joined.to_csv('./data/restaurants_integrated/output_fuzzyMatcher/results.csv', header=True, sep=";", decimal=',', float_format='%.3f')

	col_order = ["Score", "Ristorante", "Ristorante2", "Indirizzo", "Indirizzo2"]
	print(df_joined[col_order].sample(10))

	num_records = len(df_joined)
	correct_binary = (df_joined["Ristorante"] == df_joined["Ristorante2"])
	perc_correct = correct_binary.sum()/num_records

	print("The percentage of name restaurants correctly matched was {:,.1f}%".format(perc_correct*100))

if __name__ == "__main__":
	test_fuzzy_left_join()