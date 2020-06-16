import pandas as pd

def stripCols(row, cols):
    for col in cols:
        row[col] = row[col].rstrip()
    return row

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


def OpenTable_cleaner(row):

    if row['2_x'] == 'nan' or row['2_x'].isspace():
        row['2_x'] = row['2']

    return row


def SavoryCities_cleaner(row):

    if row['3'] == 'nan':
        row['3'] = row['2']
        row['2'] = 'nan'

    return row

def TimeOut_cleaner(row):

    if row['3_x'] == 'nan' and row['4_x'] == 'nan':    #Se non c'è indirizzo
        #Controllo su colonne N_y
        if row['8_y'] != 'nan':     #Numero di telefono su 8_y
            row['7_x'] = row['8_y'] #Numero di telefono
            row['8_x'] = row['3_y'] #Tipo
            row['2'] = ''
            row['3_x'] = row['5_y'] #Indirizzo
            row['4_x'] = row['6_y']
            row['6_x'] = row['7_y']
        elif row['7_y'] != 'nan':
            row['7_x'] = row['7_y'] #Numero di telefono
            row['8_x'] = row['3_y'] #Tipo
            row['2'] = ''
            row['3_x'] = row['4_y'] #Indirizzo
            row['4_x'] = row['5_y']
            row['6_x'] = row['6_y']
        elif row['6_y'] != 'nan':
            row['7_x'] = row['6_y'] #Numero di telefono
            row['8_x'] = row['3_y'] #Tipo
            row['3_x'] = row['2']
            row['2'] = ''
            row['4_x'] = row['4_y']
            row['6_x'] = row['5_y']
        elif row['3_y'] != 'nan':
            row['8_x'] = row['3_y'] #Tipo
    elif row['7_x'] != 'nan':
        row['8_x'] = row['7_x']
        row['7_x'] = row['6_x']
        row['6_x'] = row['5_x']
        row['5_x'] = row['4_x']
        row['4_x'] = row['3_x']
        row['3_x'] = row['2']
        row['3_x'] = ''
    elif row['6_x'] != 'nan':
        row['8_x'] = row['6_x']
        row['7_x'] = row['5_x']
        row['6_x'] = row['4_x']
        row['5_x'] = ''
        row['4_x'] = row['3_x']
        row['3_x'] = row['2']
        row['2'] = ''
    elif row['5_x'] != 'nan':
        row['8_x'] = row['5_x']
        row['7_x'] = row['4_x']
        row['6_x'] = row['3_x']
        row['5_x'] = ''
        row['4_x'] = ''
        row['3_x'] = row['2']
        row['2'] = ''
    elif row['3_x'] != 'nan':
        row['8_x'] = row['3_x']
        row['3_x'] = ''
        row['2'] = ''

    return row

def VillageVoice_cleaner(row):

    if row['2_x'] == 'nan':
        row['2_x'] = row['2_y'] if row['2_y'] != 'nan' else row['2']
    
    if row['3_x'] == 'nan':
        row['3_x'] = row['3_y'] if row['3_y'] != 'nan' else row['3']

    return row