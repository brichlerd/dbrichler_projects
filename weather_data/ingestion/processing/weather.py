import pandas as pd
import json

from weather_data.ingestion.api.request import WeatherRequest

def pull_weather_data(zip_code: str) -> None:
    raw_json = WeatherRequest().get_historical_weather("43205")
    # pick base flat json objects/columns & add to a dataframe
    base_field_dict = {
        "location": "resolvedAddress",
        "timezone": "timezone",
        "timezone_offset": "tzoffset",
        "current_description": "description"
    }

    # flatten the currentConditions (dict) and days (list of dicts) objects
    current = pd.json_normalize(raw_json["currentConditions"])
    forecast = pd.json_normalize(raw_json["days"])

    df_base = pd.DataFrame()
    # append the json objects in base_field_dict (values) to the dataframe naming columns as base_field_dict (keys)
    for k, v in base_field_dict.items():
        df_base[k] = [raw_json.get(v)]

    # merge the current dataframe to the base dataframe on index
    df_current = df_base.merge(current, left_index=True, right_index=True, how="left", suffixes=("","_current"))

    # if column name contains Epoch convert to datetime
    epoch_cols = df_current.columns[df_current.columns.str.contains('Epoch')]
    df_current[epoch_cols] = df_current[epoch_cols].apply(pd.to_datetime, unit='s')

    # merge the forecast dataframe to the base dataframe so each forecast day is a new row
    df_forecast = df_base.merge(forecast, how="cross", suffixes=("","_forecast"))

    # if column name contains Epoch convert to datetime
    epoch_cols = df_current.columns[df_current.columns.str.contains('Epoch')]
    df_forecast[epoch_cols] = df_forecast[epoch_cols].apply(pd.to_datetime, unit='s')
    # return both dataframes as a list
    return [df_current, df_forecast]

    def process_weather_data(df: pd.DataFrame) -> None:
        # keep only columns in hist_columns list
        hist_columns = [
            "location","timezone","timezone_offset","current_description","datetime","datetimeEpoch","tempmax","tempmin"
            ,"temp","feelslikemax","feelslikemin","feelslike","dew","humidity","precip","precipprob","precipcover",
            "preciptype","snow","snowdepth","windgust","windspeed","winddir","pressure","cloudcover","visibility",
            "solarradiation","solarenergy","uvindex","severerisk","sunrise","sunriseEpoch","sunset","sunsetEpoch",
            "moonphase"
        ]
        df = df[hist_columns]
        return None

results = pull_weather_data("43205")