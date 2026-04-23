import pandas as pd
import os

root_file = pd.read_csv('pl_elections_weather_2004_2025.csv')
folder_name = 'cleaned_weather_data' 
weather_files = [f for f in os.listdir(folder_name) if f.endswith('.csv')] 

full_paths = [os.path.join(folder_name, f) for f in weather_files]

all_dfs = []

for path in full_paths:
    df = pd.read_csv(path)
    all_dfs.append(df)

concated_weather = pd.concat(all_dfs)
concated_weather = concated_weather.rename(columns={
    'valid_time' : 'date'
})


# Some PKW snapshots weren't made during the whole hour (e.g. at 10:30:00)
# To successfully merge the weather data we need to remove the 30 minutes and assume the snapshot was made at the whole hour.
root_file['date'] = pd.to_datetime(root_file['date'])
root_file['date'] = root_file['date'].dt.floor('H')
concated_weather['date'] = pd.to_datetime(concated_weather['date'])

# Fix pandas removing the first 0 from strings at import.
root_file['TERYT'] = root_file['TERYT'].astype(str).str.zfill(6)
concated_weather['TERYT'] = concated_weather['TERYT'].astype(str).str.zfill(6)

merged_data =  pd.merge(root_file, concated_weather, how='left', on=['TERYT', 'date'])
print(merged_data.info())
merged_data.to_csv(f'merged_weather.csv', index = False, encoding= 'utf-8', header = True)

