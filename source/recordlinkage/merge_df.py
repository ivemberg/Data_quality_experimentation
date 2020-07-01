import pandas as pd
import recordlinkage
import math as m

def firstDFgenerator():
    fb = pd.read_csv("data/restaurants/gbr_splitted/FoodBuzz.csv", sep = ";")
    fb_cols = ['restaurant', 'neighborhood', 'address', 'country', 'country_code', 'type', 'cost']
    fb = fb[fb_cols]

    nym = pd.read_csv("data/restaurants/gbr_splitted/NYMag.csv", sep = ";")
    nym.rename(columns={"address": "neighborhood"}, inplace = True)

    nyt = pd.read_csv("data/restaurants/gbr_splitted/NYTimes.csv", sep = ";")
    
    ot = pd.read_csv("data/restaurants/gbr_splitted/OpenTable.csv", sep = ";")

    sc = pd.read_csv("data/restaurants/gbr_splitted/SavoryCities.csv", sep = ";")
    sc_cols = ['restaurant', 'neighborhood', 'address']
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
    compare_cl.string('restaurant', 'restaurant', threshold=0.85, label='ristorante')
    compare_cl.string('neighborhood', 'neighborhood', threshold=0.85, label='quartiere')

    features = compare_cl.compute(candidate_links, df)

    potential_matches = features[features.sum(axis=1) > 1].reset_index()
    potential_matches['Score'] = potential_matches.loc[:, ['ristorante','quartiere']].sum(axis=1)

    # MATCHES 
    account_merge = potential_matches.merge(df, left_on="level_0", right_index=True) #, how='outer'
    final_merge = account_merge.merge(df, left_on="level_1", right_index=True)
    final_merge.dropna(axis='columns', inplace=True, how = 'all') #rimuovo solo colonne tutte NaN
    final_merge.drop(columns = ['ristorante', 'quartiere', 'Score'], inplace=True) # so che sono 1 1 2

    final_list = []
    all_indexes = []
    for i, row in final_merge.iterrows():
        all_indexes.append(row.level_0)
        all_indexes.append(row.level_1)

        restaurant = row.restaurant_x
        neighborhood = row.neighborhood_x
        """
        if row.neighborhood_x.lower() == row.neighborhood_y.lower():
            neighborhood = row.neighborhood_x
        else:
            neighborhood = row.neighborhood_x + ";" + row.neighborhood_y
        """

        address = row.address_y
        country = row.country_y
        type_r = row.type_y
        cost = row.cost_y
        country_code = row.country_code_y
        new_row = (restaurant, neighborhood, address, country, country_code, type_r, cost)
        final_list.append(new_row)

    clean_merge = pd.DataFrame(final_list)
    clean_merge.columns = ["restaurant", "neighborhood", "address", "country", "country_code", "type_r", "cost"]

    df.drop(index = all_indexes, inplace = True, axis = 0)

    df = df.append(clean_merge)
    lenght = df.shape[0]
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index

    #df.to_csv("prova_deduplication.csv", header=True, sep=";", decimal=',', float_format='%.3f', index=False)
    return df
    
def secondDFgenerator():
    to = pd.read_csv("data/restaurants/gbr_splitted/TimeOut.csv", sep = ";")
    temp_to = to['in'].map(str) + ' ' + to['address1'].map(str) + '-' + to['address2'].map(str)+ to['address3'].map(str)
    temp_to = temp_to.str.replace("nan", "")
    to.drop(columns = ['in', 'address1', 'address2', 'address3'], inplace = True, axis = 1)
    to['address'] = temp_to
    to['country'] = 'New York'
    to_cols = ['restaurant', 'address', 'country', 'neighborhood', 'type', 'phone']
    to = to[to_cols]

    ad = pd.read_csv("data/restaurants/gbr_splitted/ActiveDiner.csv", sep = ";")
    
    dg = pd.read_csv("data/restaurants/gbr_splitted/DiningGuide.csv", sep = ";")

    mp = pd.read_csv("data/restaurants/gbr_splitted/MenuPages.csv", sep = ";")
    temp_mp = mp['address1'].map(str) + '-' + mp['address2'].map(str)
    temp_mp = temp_mp.str.replace("nan", "")
    mp.drop(columns = ['address1', 'address2'], inplace = True, axis = 1)
    mp['address'] = temp_mp
    mp_cols = ['restaurant', 'address']
    mp = mp[mp_cols]

    """
    NON HA INDIRIZZO

    ny = pd.read_csv("data/restaurants/gbr_splitted/NewYork.csv", sep = ";")
    ny.drop(columns = ['country_code'], inplace = True, axis = 1)
    ny['address'] = 'NaN'
    ny_cols = ['restaurant', 'address', 'country']
    ny = ny[ny_cols]
    """

    ts = pd.read_csv("data/restaurants/gbr_splitted/TasteSpace.csv", sep = ";")

    vv = pd.read_csv("data/restaurants/gbr_splitted/VillageVoice.csv", sep = ";")
    vv['country'] = 'New York'
    vv_cols = ['restaurant', 'address', 'country', 'neighborhood']
    vv = vv[vv_cols]

    df = to.append([ad,dg,mp,ts,vv])
    
    df.drop_duplicates(subset =["restaurant"], keep = "first", inplace = True) 

    lenght = df.shape[0] #14555, timeout ha 14007 record
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index
    #df.to_csv("temp_df.csv", header=True, sep=";", decimal=',', float_format='%.3f', index=False)
     
    # DEDUPLICATION
   
    indexer = recordlinkage.Index()
    indexer.sortedneighbourhood(left_on=['restaurant'])
    candidate_links = indexer.index(df)
    
    compare_cl = recordlinkage.Compare()
    compare_cl.string('restaurant', 'restaurant', threshold=0.85, label='ristorante')
    compare_cl.string('address', 'address', threshold=0.65, label='indirizzo') 

    features = compare_cl.compute(candidate_links, df)
    
    potential_matches = features[features.sum(axis=1) > 1].reset_index()
    potential_matches['Score'] = potential_matches.loc[:, ['ristorante', 'indirizzo']].sum(axis=1)
    
    # MATCHES 
    account_merge = potential_matches.merge(df, left_on="level_0", right_index=True) #, how='outer'
    final_merge = account_merge.merge(df, left_on="level_1", right_index=True)
    final_merge.dropna(axis='columns', inplace=True, how = 'all') #rimuovo solo colonne tutte NaN
    #final_merge.drop(columns = ['ristorante', 'indirizzo','Score'], inplace=True) # so che sono 1 1 2
    
    #final_merge.to_csv("final_merge_df2.csv", header=True, sep=";", decimal=',', float_format='%.3f', index=False)
    
    final_list = []
    all_indexes = []

    for i, row in final_merge.iterrows():
        all_indexes.append(row.level_0)
        all_indexes.append(row.level_1)

        restaurant = row.restaurant_x
        address = row.address_x
        country = "New York"

        # Da sistemare

        if row.type_x == row.type_y:
            type_r = str(row.type_x)
        else:
            type_r = str(row.type_x) + str(row.type_y)

        if row.phone_x == row.phone_y:
            phone = str(row.phone_x)
        else:
            phone = str(row.phone_x) + str(row.phone_y)

        neighborhood = row.neighborhood_y

        new_row = (restaurant, address, country, neighborhood, type_r, phone)
        final_list.append(new_row)

    clean_merge = pd.DataFrame(final_list)
    clean_merge.columns = ["restaurant", "address", "country", "neighborhood", "type_r", "phone"]
    
    df.drop(index = all_indexes, inplace = True, axis = 0)

    df = df.append(clean_merge)
    lenght = df.shape[0]
    new_index = []

    for i in range(lenght):
        new_index.append(i)

    df.index = new_index
    
    # df.to_csv("prova_deduplication_2.csv", header=True, sep=";", decimal=',', float_format='%.3f', index=False)
    return df

def main():

    df1 = firstDFgenerator()
    df2 = secondDFgenerator()
    print(df1, df2)

if __name__ == '__main__':
    main()