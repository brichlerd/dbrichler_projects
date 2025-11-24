import pandas as pd
import sys

# print systempath


from weather_data.ingestion.api.request import WeatherRequest

data = WeatherRequest().get_current_weather(query="Ohio")
print(data)

# ---------- FLATTEN INTO A SINGLE DICTIONARY ----------

row = {}

# flatten "location" + "request"
row.update({f"location_{k}": v for k, v in data["location"].items()})
row.update({f"request_{k}": v for k, v in data["request"].items()})

# flatten top-level "current"
for k, v in data["current"].items():

    # expand nested air_quality
    if k == "air_quality":
        row.update({f"air_quality_{aqk}": aqv for aqk, aqv in v.items()})

    # expand nested astro
    elif k == "astro":
        row.update({f"astro_{ak}": av for ak, av in v.items()})

    # expand weather descriptions + icons lists
    elif isinstance(v, list):
        row[k] = ", ".join(v)

    else:
        row[k] = v

# ---------- CREATE DATAFRAME ----------

df = pd.DataFrame([row])
print(df.head())