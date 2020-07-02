import recordlinkage
import pandas as pd
import pdb
import merge_df

# Some useful tutorial
# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html
# https://pbpython.com/record-linking.html


def linkDB(df1, df2, type, showInfo) :

	# 1 - INDEXING

	indexer = recordlinkage.Index()

	if type=="sortedneighbourhood":
		indexer.sortedneighbourhood(left_on="0_restaurant", right_on="1_restaurant")
	elif type=="full":
		indexer.full()
	elif type=="block":
		indexer.block(left_on="0_addressFix", right_on="1_addressFix") # per ora non abbiamo addressFix

	candidate_links = indexer.index(df1,df2)
	

	# 2 COMPARISON 
	comp = recordlinkage.Compare()
	comp.string('0_restaurant', '1_restaurant', threshold=0.5, label='ristorante')
	comp.string('0_address', '1_address', threshold=0.5, label='indirizzoOriginale')
	comp.string('0_neighborhood', '1_neighborhood', threshold=0.5, label='quartiere')
	
	features = comp.compute(candidate_links, df1, df2)

	# CLASSIFICATION
	# https://recordlinkage.readthedocs.io/en/latest/ref-classifiers.html#unsupervised

	# ECM Classifier
	ecm = recordlinkage.ECMClassifier()
	train_matches = ecm.fit_predict(features, match_index=None) # Train the classifier
	e_matches = ecm.predict(features)
	prob_matches = ecm.prob(features)
	print(e_matches)
	print(prob_matches)

	# K MEANS Classifier
	kmeans = recordlinkage.KMeansClassifier()
	kmeans.fit_predict(features)
	k_matches = kmeans.predict(features)
	print(k_matches)

	return 0

def main():
	
	df1 = merge_df.firstDFgenerator() 
	df1 = df1.add_prefix('0_')	
	# 0_restaurant 0_neighborhood 0_address 0_country 0_country_code 0_type 0_cost 0_type_r

	df2 = merge_df.secondDFgenerator()
	df2 = df2.add_prefix('1_')
	 #1_restaurant 1_address 1_country 1_neighbourhood 1_phone 1_type_r
	
	final_merge = linkDB(df1, df2, type="sortedneighbourhood", showInfo=True)
	# final_merge.to_csv('./data/restaurants_integrated/output_recordlinkage/final_output.csv', header=True, sep=";", decimal=',', float_format='%.3f', index=False)
	
	
	
if __name__ == "__main__":
	main()