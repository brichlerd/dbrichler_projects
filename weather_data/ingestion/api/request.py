# basic script to pull sample data from weather api
import requests
import os

from datetime import date, timedelta, datetime
from weather_data.utils.util import get_logger

logger = get_logger()

# import access key from .env file
from dotenv import load_dotenv
load_dotenv()

class WeatherRequest:

    def __init__(self):
        self.access_key = os.getenv("ACCESS_KEY")
        self.base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
        self.unit_group = "unitGroup=us"
        self.content_type="contentType=json"

    def get_current_weather(self, query: str):
        params = {
            "endpoint": "current",
            "access_key": self.access_key,
            "query": query
        }
        # url = base_url + page + access_key + query
        url = f"{self.base_url}/{params.get('endpoint')}?access_key={params.get('access_key')}&query={params.get('query')}"
        response = requests.get(url)
        # return url
        return response.json()

    # https: // weather.visualcrossing.com / VisualCrossingWebServices / rest / services / timeline
    # / 43205?unitGroup = us & key = (QWF4ZLNFTSVHNHMV9WBS7W34F
    # & contentType) = json

    def get_historical_weather(self, zip_code: str):

        try:
            url = \
                (f"{self.base_url}/"
                 f"{zip_code}?{self.unit_group}"
                 f"&key={self.access_key}"
                 f"&{self.content_type}"
                 )
        except Exception as e:
            logger.exception(e)

        response = requests.get(url)
        # return print(url)
        # return date_list
        return response.json()

WeatherRequest().get_historical_weather("Ohio&historical_date=2025-11-23")