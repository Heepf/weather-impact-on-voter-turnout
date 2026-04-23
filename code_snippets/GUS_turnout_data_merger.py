import pandas as pd
import numpy as np

files = [
    (r'BDL_GUS_cleaned\pl_ngos_per_10k_2004_2025.csv', r'ngos_per_10k_inhabitants'),
    (r'BDL_GUS_cleaned\pl_population_density_2005_2025.csv', r'population_density'),
    #(r'BDL_GUS_cleaned\pl_communes_%_of_youngest_oldest.csv', r'%_of_youngest_oldest'),
    (r'BDL_GUS_cleaned\pl_perc_of_kids_in_preschool_2004_2025.csv', r'%_of_kids_in_preschool'),
    #(r'BDL_GUS_cleaned\pl_unemployment_rate_2004_2025.csv', r'unemployment_rate'),
    (r'BDL_GUS_cleaned\pl_feminization_coefficients_2004_2025.csv', r'feminization_coefficient'),
]

main_df = pd.read_csv(r'old_data\pl_elections_turnout_weather_2005_2025.csv')

# Whenever we load the data into pandas, it assumes the TERYT code is actually an integer, and removes the first digit if it is a 0.
# We have to change it back to a string and add back the removed zero.
main_df['TERYT'] = main_df['TERYT'].astype(str).str.zfill(6)

def load_cleaned_data(path, name):
    df= pd.read_csv(path)
    df = df[['TERYT', 'year', 'value']]
    df = df.rename(columns={'value' : name})
    
    df['TERYT'] = df['TERYT'].astype(str).str.zfill(6)
    return df

for path, name in files:
    cleaned_df = load_cleaned_data(path, name)
    main_df = pd.merge(main_df, cleaned_df, how='left', on=['TERYT', 'year'])


# Unemployment data is available only at the district (powiat) level.
# We strip the last two digits of the commune TERYT to match it with the district code.

df_unemployment = load_cleaned_data(r'BDL_GUS_cleaned\pl_unemployment_rate_2004_2025.csv', 'unemployment_rate')
df_unemployment['district_TERYT'] = df_unemployment['TERYT'].str[:-2]

main_df['district_TERYT'] = main_df['TERYT'].str[:-2]
df_unemployment = df_unemployment.drop(columns=['TERYT'])

main_df = pd.merge(main_df, df_unemployment, how='left', on=['district_TERYT', 'year'])

# Age data is already pivoted (wide format) containing specific age brackets.
# We extract and rename these specific columns before merging.

df_age = pd.read_csv(r'BDL_GUS_cleaned\pl_communes_%_of_youngest_oldest.csv')
df_age = df_age[['TERYT', 'year','20-29','60+']]
df_age['TERYT'] = df_age['TERYT'].astype(str).str.zfill(6)
df_age = df_age.rename(columns={
    '20-29' : '%_of_20-29',
    '60+' : '%_of_60+'
})

main_df = pd.merge(main_df, df_age, how='left', on=['TERYT', 'year'])
main_df = main_df.drop(columns=['district_TERYT'])

cols_to_fill = [
    'income_per_inhabitant', 'ngos_per_10k_inhabitants', 'population_density',
    r'%_of_kids_in_preschool', 'feminization_coefficient', 'unemployment_rate',
    '%_of_20-29', '%_of_60+'
]

main_df[cols_to_fill] = main_df[cols_to_fill].replace(0, np.nan)
main_df[cols_to_fill] = main_df.groupby('TERYT')[cols_to_fill].ffill().bfill()

main_df.to_csv('pl_elections_weather_BDLGUS_2004_2025.csv', index = False, encoding= 'utf-8', header = True)