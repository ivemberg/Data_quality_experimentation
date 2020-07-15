import pandas as pd
import recordlinkage
import math as m

def firstDFgenerator():
    fb = pd.read_csv("./data/restaurants/gbr_splitted_google/FoodBuzz.csv", sep = ";")
    fb_cols = ['restaurant', 'neighborhood', 'address', 'addressGoogle', 'country', 'country_code', 'type', 'cost']
    fb = fb[fb_cols]

    nym = pd.read_csv("./data/restaurants/gbr_splitted_google/NYMag.csv", sep = ";")
    nym.rename(columns={"address": "neighborhood"}, inplace = True)

    nyt = pd.read_csv("./data/restaurants/gbr_splitted_google/NYTimes.csv", sep = ";")
    
    ot = pd.read_csv("./data/restaurants/gbr_splitted_google/OpenTable.csv", sep = ";")

    sc = pd.read_csv("./data/restaurants/gbr_splitted_google/SavoryCities.csv", sep = ";")
    sc_cols = ['restaurant', 'neighborhood', 'address', 'addressGoogle']
    sc = sc[sc_cols]


    df = fb.append([nym,nyt,ot,sc])

    df.drop_duplicates(subset =["restaurant", "neighborhood"], 
                        keep = "first", inplace = True) 

    lenght = df.shape[0]
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index

    # DEDUPLICATION

    indexer = recordlinkage.Index()
    indexer.sortedneighbourhood(left_on=['restaurant'])
    candidate_links = indexer.index(df)

    compare_cl = recordlinkage.Compare()
    compare_cl.string('restaurant', 'restaurant', method='jarowinkler', label='ristorante')
    compare_cl.string('neighborhood', 'neighborhood', method='jarowinkler', label='quartiere') 

    features = compare_cl.compute(candidate_links, df)

    potential_matches = features[features.sum(axis=1) > 1.75].reset_index()
    potential_matches['Score'] = potential_matches.loc[:, ['ristorante','quartiere']].sum(axis=1)

    # MATCHES 
    account_merge = potential_matches.merge(df, left_on="level_0", right_index=True) 
    final_merge = account_merge.merge(df, left_on="level_1", right_index=True)
    final_merge.dropna(axis='columns', inplace=True, how = 'all') # rimuovo solo colonne tutte NaN
    final_merge.drop(columns = ['ristorante', 'quartiere', 'Score'], inplace=True) 
    
    final_list = []
    all_indexes = []
    for i, row in final_merge.iterrows():
        all_indexes.append(row.level_0)
        all_indexes.append(row.level_1)

        restaurant = row.restaurant_x
        neighborhood = row.neighborhood_x # da controllare

        if row.addressGoogle_x == row.addressGoogle_y:
            addressGoogle = str(row.addressGoogle_x)
        elif (row.addressGoogle_x != row.addressGoogle_x): # è NaN
            addressGoogle = str(row.addressGoogle_y) 
        else:
            addressGoogle = str(row.addressGoogle_x)


        address = row.address_y
        country = row.country_y
        type_r = row.type_y
        cost = row.cost_y
        country_code = row.country_code_y
        new_row = (restaurant, neighborhood, address, addressGoogle, country, country_code, type_r, cost)
        final_list.append(new_row)

    clean_merge = pd.DataFrame(final_list)
    clean_merge.columns = ["restaurant", "neighborhood", "address", "addressGoogle", "country", "country_code", "type_r", "cost"]

    df.drop(index = all_indexes, inplace = True, axis = 0)

    df = df.append(clean_merge)
    lenght = df.shape[0]
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index
    
    return df
    
def secondDFgenerator():
    to = pd.read_csv("./data/restaurants/gbr_splitted_google/TimeOut.csv", sep = ";")
    temp_to = to['in'].map(str) + ' ' + to['address1'].map(str) + '-' + to['address2'].map(str)+ to['address3'].map(str)
    temp_to = temp_to.str.replace("nan", "")
    to.drop(columns = ['in', 'address1', 'address2', 'address3'], inplace = True, axis = 1)
    to['address'] = temp_to
    to['country'] = 'New York'
    to_cols = ['restaurant', 'address', 'addressGoogle', 'country', 'neighborhood', 'type', 'phone']
    to = to[to_cols]

    ad = pd.read_csv("./data/restaurants/gbr_splitted_google/ActiveDiner.csv", sep = ";")
    ad_cols = ['restaurant', 'address', 'addressGoogle', 'country']
    ad = ad[ad_cols]
    
    dg = pd.read_csv("./data/restaurants/gbr_splitted_google/DiningGuide.csv", sep = ";")

    mp = pd.read_csv("./data/restaurants/gbr_splitted_google/MenuPages.csv", sep = ";")
    temp_mp = mp['address1'].map(str) + '-' + mp['address2'].map(str)
    temp_mp = temp_mp.str.replace("nan", "")
    mp.drop(columns = ['address1', 'address2'], inplace = True, axis = 1)
    mp['address'] = temp_mp
    mp.rename(columns={"addressGoogle1": "addressGoogle"}, inplace = True)
    mp_cols = ['restaurant', 'address', 'addressGoogle']
    mp = mp[mp_cols]

    ts = pd.read_csv("./data/restaurants/gbr_splitted_google/TasteSpace.csv", sep = ";")
    ts_cols = ['restaurant', 'address', 'addressGoogle', 'country']
    ts = ts[ts_cols]

    vv = pd.read_csv("./data/restaurants/gbr_splitted_google/VillageVoice.csv", sep = ";")
    vv['country'] = 'New York'
    vv_cols = ['restaurant', 'address', 'addressGoogle', 'country', 'neighborhood']
    vv = vv[vv_cols]

    df = to.append([ad,dg,mp,ts,vv])
    
    df.drop_duplicates(subset =["restaurant", "addressGoogle"], keep = "first", inplace = True) 

    lenght = df.shape[0] 
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index
    
    # DEDUPLICATION
   
    indexer = recordlinkage.Index()
    indexer.sortedneighbourhood(left_on=['restaurant'])
    candidate_links = indexer.index(df)
    
    compare_cl = recordlinkage.Compare()
    compare_cl.string('restaurant', 'restaurant', method='jarowinkler', label='ristorante')
    compare_cl.exact('addressGoogle', 'addressGoogle', label = 'indirizzo')

    features = compare_cl.compute(candidate_links, df)
    
    potential_matches = features[features.sum(axis=1) > 1.75].reset_index()
    potential_matches['Score'] = potential_matches.loc[:, ['ristorante', 'indirizzo']].sum(axis=1)
    
    # MATCHES 
    account_merge = potential_matches.merge(df, left_on="level_0", right_index=True) #, how='outer'
    final_merge = account_merge.merge(df, left_on="level_1", right_index=True)
    final_merge.dropna(axis='columns', inplace=True, how = 'all') #rimuovo solo colonne tutte NaN
    final_merge.to_csv('./data/restaurants_integrated/output_recordlinkage/final_merge.csv', header=True, sep=";", decimal=',', float_format='%.3f', index=False)
    final_merge.drop(columns = ['ristorante', 'indirizzo','Score'], inplace=True) # so che sono 1 1 2
    

    final_list = []
    all_indexes = []

    for i, row in final_merge.iterrows():
        all_indexes.append(row.level_0)
        all_indexes.append(row.level_1)

        restaurant = row.restaurant_x
        address = row.address_x
        country = "New York"

        
        if row.type_x == row.type_y:
            type_r = str(row.type_x)
        elif (row.type_x != row.type_x):
            type_r = str(row.type_y)
        else:
            type_r = str(row.type_x)

        if row.phone_x == row.phone_y:
            phone = str(row.phone_x)
        elif (row.phone_x != row.phone_x):
            phone = str(row.phone_y) 
        else:
            phone = str(row.phone_x)

        if row.neighborhood_x == row.neighborhood_y:
            neighborhood = str(row.neighborhood_x)
        elif (row.neighborhood_x != row.neighborhood_x):
            neighborhood = str(row.neighborhood_y) 
        else:
            neighborhood = str(row.neighborhood_x)
        
        if row.addressGoogle_x == row.addressGoogle_y:
            addressGoogle = str(row.addressGoogle_x)
        elif (row.addressGoogle_x != row.addressGoogle_x): # è NaN
            addressGoogle = str(row.addressGoogle_y) 
        else:
            addressGoogle = str(row.addressGoogle_x)

        new_row = (restaurant, address, addressGoogle, country, neighborhood, type_r, phone)
        final_list.append(new_row)

    clean_merge = pd.DataFrame(final_list)
    clean_merge.columns = ["restaurant", "address", "addressGoogle", "country", "neighborhood", "type_r", "phone"]
    
    df.drop(index = all_indexes, inplace = True, axis = 0)

    df = df.append(clean_merge)
    lenght = df.shape[0]
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index
    
    return df


# def main():

#     df1 = firstDFgenerator()
#     df2 = secondDFgenerator()
    
#     df1.to_csv('./data/restaurants_integrated/output_recordlinkage/df1_ded_address.csv', header=True, sep=";", decimal=',', float_format='%.3f', index=False)
#     df2.to_csv('./data/restaurants_integrated/output_recordlinkage/df2_ded_addressGoogle.csv', header=True, sep=";", decimal=',', float_format='%.3f', index=False)
	

# if __name__ == '__main__':
#     main()
