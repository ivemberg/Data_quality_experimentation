import recordlinkage
import pandas as pd
import pdb

# Some useful tutorial
# https://recordlinkage.readthedocs.io/en/latest/notebooks/link_two_dataframes.html
# https://pbpython.com/record-linking.html

def deduplication() :
	return 1

def linkDB(df1, df2, type, showInfo) :
	indexer = recordlinkage.Index()
	if type=="sortedneighbourhood":
		indexer.sortedneighbourhood(left_on="0_addressFix", right_on="1_addressFix")
	elif type=="full":
		indexer.full()
	elif type=="block":
		indexer.block(left_on="0_addressFix", right_on="1_addressFix")

	candidate_links = indexer.index(df1,df2)
	print(len(candidate_links))

	# Comparison step
	comp = recordlinkage.Compare()
	comp.string('0_restaurant', '1_restaurant', threshold=0.7, label='ristorante')
	comp.string('0_address', '1_address', threshold=0.5, label='indirizzoOriginale')
	comp.exact('0_addressFix', '1_addressFix', label='indirizzoGoogle')
	features = comp.compute(candidate_links, df1, df2)

	'''
	df1['ActiveDiner_Lookup'] = df1[['0_restaurant', '0_address', '0_country']].apply(lambda x: ''.join(str(x)), axis=1)
	df2['DiningGuide_Lookup'] = df2[['1_restaurant', '1_address']].apply(lambda x: ''.join(str(x)), axis=1)

	df1_lookup = df1[['ActiveDiner_Lookup']].reset_index()
	df2_lookup = df2[['DiningGuide_Lookup']].reset_index()

	account_merge = potential_matches.merge(df1_lookup, left_on="level_0", right_index=True, how='left')
	final_merge = account_merge.merge(df2_lookup, left_on="level_1", right_index=True, how='left')
	final_merge.to_csv('test_OKAMI.csv', header=True, sep=";", decimal=',', float_format='%.3f')
	'''

	potential_matches = features[features.sum(axis=1) == 3].reset_index()
	if showInfo :
		potential_matches['Score'] = potential_matches.loc[:, ['ristorante','indirizzoOriginale','indirizzoGoogle']].sum(axis=1)

	account_merge = potential_matches.merge(df1, left_on="level_0", right_index=True, how='outer')
	final_merge = account_merge.merge(df2, left_on="level_1", right_index=True, how='outer')
	#final_merge.set_index([])

	if showInfo :
		final_merge.sort_values("Score", ascending=False)
	else :
		for index, element in enumerate(final_merge['0_restaurant']):
			if pd.isnull(element) :
				a = 1
				# pdb.set_trace()
				# non va porco cazzo di merda
				#final_merge.set_value(index, '0_restaurant', final_merge.iloc[index]['1_restaurant'])
		final_merge.drop(['level_0', 'level_1', 'ristorante', 'indirizzoOriginale', 'indirizzoGoogle', '1_restaurant', '1_address', '1_addressFix'], axis=1, inplace=True)
	return final_merge

def main():
	
	df1 = pd.read_csv("data/restaurants/gbr_splitted_google/DiningGuide_Fixed.csv", sep = ";")
	df1 = df1.add_prefix('0_')

	df2 = pd.read_csv("data/restaurants/gbr_splitted_google/ActiveDiner_Fixed.csv", sep = ";")
	df2 = df2.add_prefix('1_')

	final_merge = linkDB(df1, df2, type="sortedneighbourhood", showInfo=False)
	final_merge.to_csv('DiningGuide_ActiveDiner.csv', header=True, sep=";", decimal=',', float_format='%.3f', index=False)
	prova= final_merge['level_0'].nunique()
	print(prova)
if __name__ == "__main__":
	main()