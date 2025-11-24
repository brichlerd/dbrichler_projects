# basic script to pull sample data from weather api
import requests
import os

# import access key from .env file
from dotenv import load_dotenv
load_dotenv()

class WeatherRequest:

    def __init__(self):
        self.access_key = os.getenv("ACCESS_KEY")
        self.base_url = "http://api.weatherstack.com"

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

# print(WeatherRequest().get_current_weather(query="Ohio"))
# response = WeatherRequest().get_current_weather(query="Ohio")

