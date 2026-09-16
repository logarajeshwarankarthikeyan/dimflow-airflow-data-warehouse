import pandas as pd



def extract_shows_data(input_path):
    """
    Extracts shows data from a JSON file and returns it as a pandas DataFrame.
    
    Args:
        input_path (str): input_path to the JSON file containing shows data.
    
    Returns:
        pd.DataFrame: DataFrame containing the shows data.
    """

    data = pd.read_json(input_path)

    
    return data



def extract_show_data(input_path):
    shows_data = extract_shows_data(input_path)
    country_data = shows_data['network'].to_frame()
    df = pd.DataFrame({'tvmaze_id' : shows_data['id']  , 
                       'show_name' : shows_data['name'] ,
                       'country_id' : country_data['network'].apply(lambda x: (x['id'] if isinstance(x, dict) else None)) ,
                       'country_name' : country_data['network'].apply(lambda x: (x['country']['name'] if isinstance(x, dict) else None)) ,
                       'country_code' : country_data['network'].apply(lambda x: (x['country']['code'] if isinstance(x, dict) else None)) ,
                      })
    show_df = df[['tvmaze_id', 'show_name']].dropna(subset=['tvmaze_id']).drop_duplicates(subset=['tvmaze_id'])
    country_df = df[['country_name']].dropna(subset=['country_name']).drop_duplicates(subset=['country_name'])
    
    return show_df.to_dict('records'), country_df.to_dict('records')
