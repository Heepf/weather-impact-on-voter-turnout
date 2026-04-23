import pandas as pd
from rapidfuzz import process, fuzz


#######
# --- DATA PREPARATION
#######


df_names = pd.read_csv('PE_2014_1.csv')

# The 'TERYT' column in this dataset doesn't hold actual numeric codes yet. 
# It contains makeshift IDs built during scraping (format: "district_name; commune_name").
new_columns = df_names['TERYT'].str.split(';', expand=True)
df_names['district'] = new_columns[0]
df_names['commune'] = new_columns[1]

# The TERYT codes will be extracted from the 2023 election data file.
# For the elections after 2020, PKW provides .xlsx and .csv files with all the turnout data needed.
df_teryt = pd.read_csv(r"wyniki_gl_na_kand_po_gminach_utf8.csv", sep=';')

# Keep only standard communes. Drop voting districts abroad ('zagranica') and on ships ('statki').
df_teryt = df_teryt[["TERYT_code", "commune_type", "district", "commune", "province"]]
df_teryt = df_teryt.loc[~df_teryt['commune_type'].isin(['statki', 'zagranica'])]


#######
# --- DATA NORMALIZATION
#######


def clean_names(df, col_name):
    return (df[col_name]
                .str.lower()
                .str.strip())


df_names['district'] = clean_names(df_names, 'district')
df_names['commune'] = clean_names(df_names, 'commune')

df_teryt['district'] = clean_names(df_teryt, "district")
df_teryt['commune'] = clean_names(df_teryt, "commune")


#######
# --- FUZZY MATCHING FOR MISMATCHED COMMUNES
#######


# First attempt to merge on exact administrative names.
df_merged = pd.merge(df_teryt, df_names, how='left', on=['commune', 'district'])

mask = df_merged[['TERYT']].isna().any(axis=1)
df_missing_values = df_merged[mask].copy()

# Missing matches are usually due to typos or naming conventions (e.g. "gm. X" vs "m. X").
# Use RapidFuzz to find the closest matching commune name.
def rapid_fuzz(name, choices_list):
    result = process.extractOne(name, choices=choices_list, scorer=fuzz.partial_ratio)

    if result and result[1] > 90:
        return result[0]
    return None

comm_choices = df_names['commune'].unique().tolist()
df_missing_values.loc[:, 'matched_commune'] = df_missing_values['commune'].apply(rapid_fuzz, choices_list=comm_choices)

mapping = dict(zip(df_missing_values['commune'].dropna(), df_missing_values['matched_commune'].dropna() ))
df_teryt['commune'] = df_teryt['commune'].replace(mapping)


#######
# --- MANUAL OVERRIDES & FINAL MERGE
#######

df_teryt.loc[(df_teryt['district'] == 'wąbrzeski') & (df_teryt['commune'] == 'gm. ryn'), 'commune'] = 'gm. wąbrzeźno'
df_teryt.loc[(df_teryt['district'] == 'brzeski') & (df_teryt['commune'] == 'gm. brzeg dolny'), 'commune'] = 'gm. brzeg'
df_teryt.loc[(df_teryt['district'] == 'stalowowolski') & (df_teryt['commune'] == 'wola'), 'commune'] = 'gm. stalowa wola'
df_teryt.loc[(df_teryt['district'] == 'lubliniecki') & (df_teryt['commune'] == 'm. lublin'), 'commune'] = 'gm. lubliniec'
df_teryt.loc[(df_teryt['district'] == 'pilski') & (df_teryt['commune'] == 'gm. piława górna'), 'commune'] = 'gm. piła'

# Final merge using the corrected, fuzzy-matched names.
df_final = pd.merge(df_teryt, df_names, how='left', left_on=['commune', 'district'], right_on=['commune', 'district'])

# Restore proper 6-digit TERYT format (Pandas strips leading zeros on import).
df_final['TERYT_code'] = df_final['TERYT_code'].astype(str).str.zfill(6)

df_final['commune'] = df_final['commune'].str.replace('gm. ', '')
df_final['commune'] = df_final['commune'].str.replace('m. ', '')

df_final.to_csv('PE_2014_2_MERGED.csv', index = False, encoding= 'utf-8', header = True)
