import recordlinkage
import pandas as pd
import pdb
import merge_df

# Some useful tutorial
# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html
# https://pbpython.com/record-linking.html


def linkDB(df1, df2, type, classifier):

	# 1 - INDEXING

	indexer = recordlinkage.Index()

	if type == "sortedneighbourhood":
		indexer.sortedneighbourhood(left_on="0_restaurant", right_on="1_restaurant")
	elif type == "full":
		indexer.full()
	elif type == "block":
		indexer.block(left_on="0_addressGoogle", right_on="1_addressGoogle")

	candidate_links = indexer.index(df1, df2)

	test_pairs = candidate_links[0:50]
	
	#https://recordlinkage.readthedocs.io/en/latest/annotation.html
	"""
	df1.columns = df1.columns.str.replace(r'0_', '')
	df2.columns = df2.columns.str.replace(r'1_', '')
	
	recordlinkage.write_annotation_file(
		"PIPPO.json", candidate_links[0:50], df1, df2, dataset_a_name="firstDF", dataset_b_name="secondDF")
	
	df1 = df1.add_prefix('0_')
	df2 = df2.add_prefix('1_')
	"""
	
	annotations = recordlinkage.read_annotation_file('result.json')

	# 2 - COMPARISON
	comp = recordlinkage.Compare()
	comp.string('0_restaurant', '1_restaurant', threshold=0.95,
	            method='jarowinkler', label='ristorante')
	comp.string('0_neighborhood', '1_neighborhood', method='jarowinkler',
	            threshold=0.85, label='quartiere')
	comp.exact('0_addressGoogle', '1_addressGoogle', label='indirizzoGoogle')

	features = comp.compute(candidate_links, df1, df2)
	
	# 3 - CLASSIFICATION
	# https://recordlinkage.readthedocs.io/en/latest/ref-classifiers.html#unsupervised

	matches = []

	if classifier == "ecm":
		ecm = recordlinkage.ECMClassifier(
			init='jaro', binarize=None, max_iter=100, atol=0.0001, use_col_names=True)
		ecm.fit_predict(features, match_index=None)  # Train the classifier
		e_matches = ecm.predict(features)
		for i, j in e_matches:
			record_1 = df1.loc[i]
			record_2 = df2.loc[j]
			record = tuple(record_1) + tuple(record_2)
			matches.append(record)
	elif classifier == "kmeans":
		kmeans = recordlinkage.KMeansClassifier()
		kmeans.fit_predict(features)
		k_matches = kmeans.predict(features)
		for i, j in k_matches:
			record_1 = df1.loc[i]
			record_2 = df2.loc[j]
			record = tuple(record_1) + tuple(record_2)
			matches.append(record)
	
	head = tuple(df1.head()) + tuple(df2.head())
	result = pd.DataFrame(matches)
	result.columns = head
	
	# 4 - EVALUATION
	
	if classifier == "ecm":
		test_matches = ecm.predict(test_pairs)
		print(test_matches)
		tp = recordlinkage.true_positives(annotations.links, test_matches)
		cm = recordlinkage.confusion_matrix(annotations.links, test_matches, total=None)
	elif classifier == "kmeans":
		test_matches = kmeans.predict(test_pairs)
		tp = recordlinkage.true_positives(annotations.links, test_matches)
		cm = recordlinkage.confusion_matrix(annotations.links, test_matches, total=None)

	print(tp)
	print(cm)

	return result


def main():

	df1 = merge_df.firstDFgenerator()
	df1 = df1.add_prefix('0_')

	df2 = merge_df.secondDFgenerator()
	df2['country_code'] = 'NY'
	df2['cost'] = ''
	df2_cols = ['restaurant', 'neighborhood', 'address', 'addressGoogle', 'country', 'country_code', 'type', 'cost', 'type_r']
	df2 = df2[df2_cols]
	df2 = df2.add_prefix('1_')

	ecm_result = linkDB(df1, df2, type="sortedneighbourhood", classifier="ecm")
	km_result = linkDB(df1, df2, type="sortedneighbourhood", classifier="kmeans")
	
	
	ecm_result.to_csv('./data/restaurants_integrated/output_recordlinkage/final_matches_ecm_addressGoogle.csv',
	                  header=True, sep=";", decimal=',', float_format='%.3f', index=False)
	km_result.to_csv('./data/restaurants_integrated/output_recordlinkage/final_matches_km_addressGoogle.csv',
	                 header=True, sep=";", decimal=',', float_format='%.3f', index=False)


if __name__ == "__main__":
	main()
