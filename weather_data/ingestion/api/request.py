# basic script to pull sample data from weather api
import requests
import os

# import access key from .env file
from dotenv import load_dotenv
load_dotenv()

class Request:
    def __init__(self):
        self.access_key = os.getenv("ACCESS_KEY")
        self.base_url = "http://api.weatherstack.com"

    def get_current_weather(self):
        params = {
            "page": "current",
            "access_key": self.access_key,
            "query": "Ohio"
        }
        # url = base_url + page + access_key + query
        url = f"{self.base_url}/{params.get('page')}?access_key={params.get('access_key')}&query={params.get('query')}"
        response = requests.get(url)
        # return url
        return response.json()

print(Request().get_current_weather())

