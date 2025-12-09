import pandas as pd
import json
import datetime as dt

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
df = results[1]

## print datetime field
print(df['datetimeEpoch'].iloc[0])

# convert to date
df['date'] = df['datetimeEpoch'].dt.date
print(df['date'].iloc[0])

# convert to datetime with time 00:00:00
df['datetime_pd'] = pd.to_datetime(df['date'])
print(df['datetime_pd'].iloc[0])

## find time b/w sunrise and sunset
# create dataframe with sunrise and sunset columns converted to datetime
df_sun = df[['sunriseEpoch', 'sunsetEpoch']].copy()
for col in df_sun.columns:
    df_sun[col] = pd.to_datetime(df_sun[col])

df_sun['daylight_duration'] = df_sun['sunriseEpoch'] - df_sun['sunsetEpoch']
df_sun['daylight_seconds'] = df_sun['daylight_duration'].dt.total_seconds().abs()
df_sun['daylight_hours'] = df_sun['daylight_seconds'] / 3600

# extract day of week as string (e.g., Monday, Tuesday)
df_sun['day_of_week'] = df['sunriseEpoch'].dt.day_name()
print(df_sun['day_of_week'].iloc[0])

# return list of days where daylight durations is less than 9.35 hours
long_days = []
df_sun['is_long_day'] = df_sun['daylight_hours'] < 9.35
long_days.append(df_sun['day_of_week'][df_sun['is_long_day']].tolist())

# return list of daylight durations for mondays
monday_durations = []
df_mondays = df_sun[df_sun['day_of_week'] == 'Monday']
monday_durations.append(df_mondays['daylight_hours'].tolist())

## find difference b/w daylight seconds between the same day of week across weeks
daylight_day_of_week_diff = {}
for day in df_sun['day_of_week'].unique():
    df_day = df_sun[df_sun['day_of_week'] == day]
    # find diff from previous value
    daylight_day_of_week_diff[day] = df_day['daylight_seconds'].diff().tolist()
print(daylight_day_of_week_diff)

avg_daylight_day_of_week_diff = {}
for day in df_sun['day_of_week'].unique():
    df_day = df_sun[df_sun['day_of_week'] == day]
    # find diff average diff
    avg_daylight_day_of_week_diff[day] = df_day['daylight_seconds'].diff()
    avg_daylight_day_of_week_diff[day] = avg_daylight_day_of_week_diff[day].mean()

## create single column with difference b/w daylight seconds between the same day of week across weeks
df_dow = df_sun.copy()
df_dow['daylight_seconds_diff'] = df_dow.groupby('day_of_week')['daylight_seconds'].diff()
print(df_dow[['day_of_week', 'daylight_seconds', 'daylight_seconds_diff']])

# print(df_dow[df_dow['day_of_week'] == 'Monday'])


