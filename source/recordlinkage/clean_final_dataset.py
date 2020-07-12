import pandas as pd


def fClean(row: pd.Series) -> pd.Series:
    # Restaurant name
    if len(row['1_restaurant']) > len(row['0_restaurant']):
        row['0_restaurant'] = row['1_restaurant']

    # Type
    if not row['0_type']:
        row['0_type'] = row['0_type_r']
    if not row['1_type']:
        row['1_type'] = row['1_type_r']
    if not row['0_type']:
        row['0_type'] = row['1_type']
    elif row['0_type'] and row['1_type'] and row['0_type'] != row['1_type']:
        row['0_type'] = f"{row['0_type']} \\ {row['1_type']}"

    # Neighborhood
    if not row['0_neighborhood']:
        row['0_neighborhood'] = row['1_neighborhood']

    # Address
    if not row['0_addressGoogle']:
        row['0_address'] = row['0_addressGoogle']
    elif not row['1_addressGoogle']:
        row['0_address'] = row['1_addressGoogle']
    elif not row['0_address'] and row['1_address']:
        row['0_address'] = row['1_address']

    # Country is always NY
    row['0_country'] = 'New York'
    row['0_country_code'] = 'NY'

    return row


def final_clean(dfIn: pd.DataFrame) -> pd.DataFrame:
    df = dfIn.copy()
    df = df.astype(str)
    df.replace('nan', '', inplace=True)
    df = df.apply(fClean, axis=1)
    df = df.drop(['0_addressGoogle', '0_type_r', '1_restaurant', '1_address',
                  '1_addressGoogle', '1_country', '1_neighborhood', '1_type', '1_type_r'], axis=1)
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    df.drop_duplicates(inplace=True)
    df.rename(columns={'0_restaurant': 'Name',
                       '0_neighborhood': 'Neighborhood',
                       '0_address': 'Address',
                       '0_country': 'Country',
                       '0_country_code': 'Country code',
                       '0_type': 'Type',
                       '0_cost': 'Cost',
                       '1_phone': 'Phone'}, inplace=True)
    return df

def main():
    dfToClean = pd.read_csv('./data/restaurants_integrated/output_recordlinkage/final_matches_km_addressGoogle.csv', sep=';')
    dfCleaned = final_clean(dfToClean)
    dfCleaned.to_csv('./data/restaurants_integrated/output_recordlinkage/final_matches_cleaned.csv', sep=';', index=None)

if __name__ == '__main__':
    main()