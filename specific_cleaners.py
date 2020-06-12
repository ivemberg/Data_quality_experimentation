import pandas as pd

def FoodBuzz_cleaner(row):

    #Fill empty columns
    if not row['3_x'] or row['3_x'].isspace():
        row['3_x'] = row['3_y']
    if not row['4_x'] or row['4_x'].isspace():
        row['4_x'] = row['4_y']
    if not row['5_x'] or row['5_x'].isspace():
        row['5_x'] = row['5_y']
    if not row['6_x'] or row['6_x'].isspace():
        row['6_x'] = row['6_y']
    if not row['7_x'] or row['7_x'].isspace():
        row['7_x'] = row['7_y']

    #Merge over-separated fields    
    if (row['8_x'] != 'nan' and (not row['8_x'].isspace())) and (row['9_x'] != 'nan' and (not row['9_x'].isspace())):   #If cell not empty but not only white spaces
        row['2'] = row['2'] + ', ' + row['3_x'] + ', ' + row['4_x']
        row['3_x'] = row['5_x']
        row['4_x'] = row['6_x']
        row['5_x'] = row['7_x']
        row['6_x'] = row['8_x']
        row['7_x'] = row['9_x']
    elif row['8_x'] != 'nan' and (not row['8_x'].isspace()):
        row['2'] = row['2'] + ', ' + row['3_x']
        row['3_x'] = row['4_x']
        row['4_x'] = row['5_x']
        row['5_x'] = row['6_x']
        row['6_x'] = row['7_x']
        row['7_x'] = row['8_x']

    return row

def MenuPages_cleaner(row):

    if row['6_x'] == 'nan' or row['6_x'].isspace():
        row['6_x'] = row['6_y']

    return row



def NYMag_cleaner(row):

    if row['2_x'] == 'nan' or row['2_x'].isspace():
        row['2_x'] = row['2_y']

    return row