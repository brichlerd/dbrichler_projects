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
        return response.json()