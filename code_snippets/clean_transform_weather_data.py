import xarray as xr
import pandas as pd
import numpy as np
import os
from scipy.spatial import KDTree

# file path and corresponding turnout reporting hours by PKW
weather_files = [
    (r'raw_weather_data\2005_09_25_weather_data.nc', [7, 10, 16, 21]),
    (r'raw_weather_data\2005_10_09_weather_data.nc', [7, 10, 16, 21]),
    (r'raw_weather_data\2005_10_23_weather_data.nc', [7, 10, 16, 21]),
    (r'raw_weather_data\2007_10_21_weather_data.nc', [7, 10, 16, 21]),
    (r'raw_weather_data\2009_06_07_weather_data.nc', [7, 12, 18, 21]),
    (r'raw_weather_data\2010_06_20_weather_data.nc', [7, 8, 13, 17, 21]),
    (r'raw_weather_data\2010_07_04_weather_data.nc', [7, 8, 13, 17, 21]),
    (r'raw_weather_data\2011_10_09_weather_data.nc', [7, 9, 14, 18, 21]),
    (r'raw_weather_data\2014_05_25_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2015_05_10_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2015_05_24_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2015_10_25_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2019_05_26_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2019_10_13_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2020_06_28_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2020_07_12_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2023_10_15_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2024_06_09_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2025_05_18_weather_data.nc', [7, 12, 17, 21]),
    (r'raw_weather_data\2025_06_01_weather_data.nc', [7, 12, 17, 21])
]


root_file = 'pl_elections_weather_2004_2025.csv'

root_df = pd.read_csv(root_file)
df_unique_locations = root_df[['TERYT', 'Latitude', 'Longitude']].drop_duplicates()


def sanity_checker(df):
    """
    Checks the dataset for logical duplicates and severe negative precipitation values 
    that might indicate merging or interpolation errors.
    """

    logical_duplicates = df.duplicated(subset=['TERYT', 'valid_time']).sum()
    precipitation_errors = df[df['total_precipitation'] < -0.002]

    if precipitation_errors.empty and logical_duplicates == 0:
        print("Logic is healthy.")
    else:
        print(precipitation_errors[['TERYT', 'valid_time', 'total_precipitation']].head())
        raise ValueError(f"Found {len(precipitation_errors)} negative values {logical_duplicates} logical duplicates.")
    


for filepath, timestamps in weather_files:

    all_communes = []
    ds = xr.open_dataset(filepath)

    poland_mask = (
        ((ds.latitude >= 48) & (ds.latitude <= 55) ) &
        ((ds.longitude >= 14) & (ds.longitude <= 25))
    )

    ds = ds.where(poland_mask, drop=True)

    cerra_cords = np.stack([ds.latitude.values.ravel(), ds.longitude.values.ravel()], axis=1)
    tree = KDTree(cerra_cords)

    print(f'Analysis started! Current file: {filepath}')

    for i, row in enumerate(df_unique_locations.itertuples()):
        
        commune_lat = row.Latitude
        commune_lon = row.Longitude

        # Do a knn analysis.
        # An average Polish commune is about 125 km^2. One CERRA pixel is 30.25 km^2,
        # Because the commune shapes can be irregular, and weather phenomena in general cover long distances, 
        # I decided to look for the 6 nearest CERRA pixels (181.5km^2) and average them.

        distances, indicies = tree.query((commune_lat, commune_lon), k=6)
        y_idx, x_idx = np.unravel_index(indicies, ds.latitude.shape)
        
        subset = ds.isel(
            x=xr.DataArray(x_idx, dims="points"),
            y=xr.DataArray(y_idx, dims="points")
        )

        df = subset.to_dataframe().reset_index()

        df = df.dropna(subset=['t2m'])
        df = df.drop(columns=['expver'])

        df = df.rename(columns={
            'fg10' : 'wind_gusts',
            'r2' : 'rel_humidity',
            't2m' : 'temperature',
            'tcc' : 'total_cloud_cover',
            'tp' : 'total_precipitation'
        })

        # Change to the polish timezone.
        df['valid_time'] = df['valid_time'].dt.tz_localize('GMT').dt.tz_convert('Europe/Warsaw').dt.tz_localize(None)
        # Temperature is reported in Kelvin, so we need to convert it to Celsius.
        df['temperature'] = df['temperature'] - 273.15
        df['TERYT'] = row.TERYT

        # CERRA logic: the precipitation is accumulated over a 3h forecast window.
        # To get the actual precipitation for a specific hour, we must calculate the difference from the previous hour (diff).
        # We need to be sure the precipitation values are sorted by date.

        df = df.sort_values('valid_time', ascending=True)

        df = df.groupby(by='valid_time').mean().reset_index()

        df['prev_precipation'] = df['total_precipitation'].diff()
        df.loc[df['prev_precipation'].isna(), 'prev_precipation'] = 0

        # The modulo 3 logic resets this calculation at the start of each new forecast cycle
        df['step'] = np.arange(len(df)) % 3 + 1

        df['total_precipitation'] = np.where(
            df['step'] == 1,
            df['total_precipitation'],
            df['prev_precipation']
        )

        # Due to float calculations the total precipitation can sometimes end up in tiny negatives.
        # Simply change those numbers to 0. If the number is less than -0.001, then leave it as it is,
        # as it is a potential error than needs to be caught during the sanity checks.

        df['total_precipitation'] = np.where(
            df['total_precipitation'].between(-0.001, 0),
            0,
            df['total_precipitation']
        )

        df['TERYT'] = df['TERYT'].astype(str).str.replace('.0', '').str.zfill(6)
        df = df.drop(columns=['prev_precipation', 'step', 'points', 'latitude', 'longitude'], errors='ignore')
        all_communes.append(df)

    df_all_communes = pd.concat(all_communes)
    sanity_checker(df_all_communes)

    # Align the hourly weather data with the official voter turnout reporting hours used by PKW (State Electoral Commission).
    # Weather is aggregated up to the moment PKW announced the turnout snapshot.
    df_all_communes['pkw_hour'] = pd.cut(df_all_communes['valid_time'].dt.hour,
                            bins=timestamps,
                            labels=timestamps[1:],
                            include_lowest=True)

    df_all_communes = df_all_communes.dropna(subset='pkw_hour')
    df_all_communes['pkw_hour'] = df_all_communes['pkw_hour'].astype(str)

    df_all_communes = df_all_communes.groupby(by=['TERYT', 'pkw_hour']).agg({
        'TERYT' : 'first',
        'valid_time' : 'last',
        'wind_gusts' : 'mean',
        'rel_humidity' : 'mean',
        'temperature' : 'mean',
        'total_cloud_cover' : 'mean',
        'total_precipitation' : 'sum',
    })

    sanity_checker(df_all_communes)

    clean_file_name = f'cleaned_{os.path.basename(filepath)}'
    clean_file_name = clean_file_name.replace('.nc', '.csv')

    df_all_communes.to_csv(f'cleaned_weather_data/{clean_file_name}', index = False, encoding= 'utf-8', header = True)

    
    


