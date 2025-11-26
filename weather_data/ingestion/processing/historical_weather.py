import pandas as pd
import json

from weather_data.ingestion.api.request import WeatherRequest

def process(zip_code: str) -> pd.DataFrame:
    # pull json file and store it as a variable
    raw_json = WeatherRequest().get_historical_weather(zip_code)
    # pick base flat json objects/columns & add to a dataframe
    base_field_dict = {
        "location": "resolvedAddress",
        "timezone": "timezone",
        "timezone_offset": "tzoffset",
        "current_description": "description"
        # "current_": "currentConditions",
        # "forecast_": "days"
    }
    df = pd.DataFrame()
    for k, v in base_field_dict.items():
        df[k] = raw_json.get(v)

    return df

def flatten_weatherstack(data):
    row = {}

    # --- Safe get for location ----
    location = data.get("resolvedAddress") or {}
    if type(location) is dict:
        for k, v in location.items():
            row[f"resolvedAddress_{k}"] = v
    else:
        row["resolvedAddress"] = location

    # --- Safe get for desc ----
    description = data.get("description") or {}
    if type(description) is dict:
        for k, v in description.items():
            row[f"description{k}"] = v
    else:
        row["description"] = description

    # --- Safe current weather ----
    current = data.get("currentConditions") or {}
    for k, v in current.items():

        # nested dict handling
        if isinstance(v, dict):
            for subk, subv in v.items():
                row[f"{k}_{subk}"] = subv

        # list → comma string
        elif isinstance(v, list):
            row[k] = ", ".join(v)

        else:
            row[k] = v

    # --- Safe forecast weather ---
    # start = data.find("{")
    # end = data.rfind("}") + 1
    # json_text = data[start:end]
    # forecast_data = json.loads(json_text)
    forecast = pd.json_normalize(data["days"])
    # forecast = data.get("days") or {}

    #create list of days as str values 0 thru 15 with leading zeros
    # date_key = []
    # count = 0
    # for d in forecast:
    #     if count == 0:
    #         date_key.append("00")
    #     elif count > 9:
    #         date_key.append(f"{count}")
    #     else:
    #         date_key.append(f"0{count}")
    #     count += 1
    #
    # for v in forecast:
    #     forecast[f"{v}"] = v
        # row["forecast_date"] = forecast[date_key]


    return forecast


# raw_json = WeatherRequest().get_historical_weather(
#     zip_code="43205"
# )

df = process("43205")
