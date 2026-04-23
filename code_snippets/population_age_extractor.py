import pandas as pd

df= pd.read_csv(r'BDL GUS\population_age.csv', sep=';')

age_map = {
    '20-24' : '20-29',
    '25-29' : '20-29',
    '60-64' : '60+',
    '65-69' : '60+',
    '70 i więcej' : '60+'
}

def clean_data(df_to_clean):

    # The logic below cleans the TERYT code.
    # Whenever we load the data into pandas, it assumes the TERYT code is actually an integer, and removes the first digit if it is a 0.
    # We have to change it back to a string and add back the removed zero.
    df_to_clean['Kod'] = df_to_clean['Kod'].astype(str).str.zfill(7)
    
    # Remove the last digit (unit type indicator)
    df_to_clean['Kod'] = df_to_clean['Kod'].str[:-1]
    df_to_clean['Rok'] = df_to_clean['Rok'].astype(int)
    #df_to_clean['Wartosc'] = df_to_clean['Wartosc'].str.replace(',', '.').astype(float)

    return df_to_clean[['Kod', 'Wiek' ,'Nazwa', 'Rok', 'Wartosc']]

df = clean_data(df)

# We only need the youngest and the oldest voters.
# The logic belows creates baskets regarding that.
df = df.rename(columns={
    'Kod' : 'TERYT',
    'Wiek' : 'age',
    'Nazwa' : 'title',
    'Rok' : 'year',
    'Wartosc' : 'value'
})
df['basket'] = df['age'].map(age_map)

df_population = df.groupby(['TERYT', 'year'], as_index=False)['value'].sum()
df = df.merge(df_population, how= 'left', on=['TERYT', 'year'])

# We need to calculate the ratio between the age subgroups and the total population.
df = df.rename(columns={
        'value_x' : 'group_count',
        'value_y' : 'total_population'
})

df['%_of_population'] = (df['group_count'] / df['total_population']) * 100

df = df[['TERYT', 'title', 'year', 'basket', '%_of_population']]

# Remove the age brackets that fall outside of our interest circle.
df = df.dropna(subset=['basket'])

df = df.groupby(['TERYT', 'year', 'basket'], as_index=False)['%_of_population'].sum()
df = df[df['year'] > 2003]
df = df.sort_values(['TERYT', 'year'])

df_wide = df.pivot_table(
    index = ['TERYT', "year"],
    columns = 'basket',
    values = '%_of_population'
).reset_index()

df_wide.to_csv('pl_communes_%_of_youngest_oldest.csv', index = False, encoding= 'utf-8', header = True)