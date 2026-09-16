import pandas as pd
import os



def read_data_json(file_path):
    """
    Read JSON data from a file and return it as a DataFrame.
    """
    if os.path.exists(file_path):
        df = pd.read_json(file_path)
        return df
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    

def transform_city_data(file_path):
    df = read_data_json(file_path)
    city_df = pd.DataFrame({
        'city_id': df['id'],
        'city': df['name'],
        'country_code': df['sys'].apply(lambda x: x.get('country', None)),
        'latitude': df['coord'].apply(lambda x: x.get('lat', None)),
        'longitude': df['coord'].apply(lambda x: x.get('lon', None)),
    })
    return city_df.drop_duplicates(subset=['city_id']).to_dict('records')

def transform_weather_data(file_path):
    df = read_data_json(file_path)
    weather_df = pd.DataFrame({
        'city_id': df['id'],
        'date_id': df['dt'].apply(lambda x: pd.to_datetime(x, unit='s').date().strftime('%Y-%m-%d')),
        'temperature': df['main'].apply(lambda x: x.get('temp', None)),
        'humidity': df['main'].apply(lambda x: x.get('humidity', None)),
    })
    return weather_df.drop_duplicates(subset=['city_id', 'date_id']).to_dict('records')


def transform_date_data():

    date_range = pd.date_range(start='2024-01-01', end='2025-12-31')

    date_df = pd.DataFrame({
    'date_id': date_range.date.astype(str),
    'year': date_range.year,
    'month': date_range.month,
    'day': date_range.day,
    'day_of_week': date_range.weekday + 1,  # Monday=1, Sunday=7
    'is_weekend': date_range.weekday >= 5   # Saturday/Sunday = True
})

    return date_df.drop_duplicates(subset=['date_id']).to_dict('records')
