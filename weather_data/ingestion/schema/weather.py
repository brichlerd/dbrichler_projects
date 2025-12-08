from datetime import datetime

from sqlalchemy import TIMESTAMP, func, text
from sqlmodel import Field, SQLModel

from weather_data.ingestion.schema.base import BaseTable

from typing import Optional

class WeatherForecast(BaseTable):
    """"Weather forecast data"""
    __tablename__ = "weather_forecast"

    location: str = Field(default=None, description="Location name or identifier", nullable=True)
    timezone: Optional[str] = Field(default=None, description="Timezone name (e.g. 'America/Los_Angeles')", nullable=True)
    timezone_offset: Optional[int] = Field(default=None, description="Timezone offset in seconds from UTC", nullable=True)
    current_description: Optional[str] = Field(default=None, description="Short current weather description", nullable=True)

    datetime: datetime = Field(default=None, description="Local datetime for this forecast entry", nullable=True)
    datetimeEpoch: int = Field(default=None, description="Epoch seconds for datetime", nullable=True)

    tempmax: Optional[float] = Field(default=None, description="Maximum temperature", nullable=True)
    tempmin: Optional[float] = Field(default=None, description="Minimum temperature", nullable=True)
    temp: Optional[float] = Field(default=None, description="Observed or average temperature", nullable=True)

    feelslikemax: Optional[float] = Field(default=None, description="Maximum feels-like temperature", nullable=True)
    feelslikemin: Optional[float] = Field(default=None, description="Minimum feels-like temperature", nullable=True)
    feelslike: Optional[float] = Field(default=None, description="Feels-like temperature", nullable=True)

    dew: Optional[float] = Field(default=None, description="Dew point", nullable=True)
    humidity: Optional[float] = Field(default=None, description="Relative humidity (percent)", nullable=True)

    precip: Optional[float] = Field(default=None, description="Precipitation amount (e.g. mm)", nullable=True)
    precipprob: Optional[float] = Field(default=None, description="Probability of precipitation (percent)", nullable=True)
    precipcover: Optional[float] = Field(default=None, description="Percent area covered by precipitation", nullable=True)
    preciptype: Optional[str] = Field(default=None, description="Precipitation types (comma separated)", nullable=True)

    snow: Optional[float] = Field(default=None, description="Snowfall amount", nullable=True)
    snowdepth: Optional[float] = Field(default=None, description="Snow depth", nullable=True)

    windgust: Optional[float] = Field(default=None, description="Peak wind gust", nullable=True)
    windspeed: Optional[float] = Field(default=None, description="Wind speed", nullable=True)
    winddir: Optional[float] = Field(default=None, description="Wind direction in degrees", nullable=True)

    pressure: Optional[float] = Field(default=None, description="Atmospheric pressure (e.g. hPa)", nullable=True)
    cloudcover: Optional[float] = Field(default=None, description="Cloud cover percentage", nullable=True)
    visibility: Optional[float] = Field(default=None, description="Visibility distance (e.g. km)", nullable=True)

    solarradiation: Optional[float] = Field(default=None, description="Instantaneous solar radiation", nullable=True)
    solarenergy: Optional[float] = Field(default=None, description="Accumulated solar energy", nullable=True)
    uvindex: Optional[float] = Field(default=None, description="UV index", nullable=True)
    severerisk: Optional[float] = Field(default=None, description="Severe weather risk score", nullable=True)

    sunrise: Optional[datetime] = Field(default=None, description="Local sunrise time", nullable=True)
    sunriseEpoch: Optional[int] = Field(default=None, description="Epoch seconds for sunrise", nullable=True)
    sunset: Optional[datetime] = Field(default=None, description="Local sunset time", nullable=True)
    sunsetEpoch: Optional[int] = Field(default=None, description="Epoch seconds for sunset", nullable=True)

    moonphase: Optional[float] = Field(default=None, description="Moon phase (0-1)", nullable=True)

    conditions: Optional[str] = Field(default=None, description="Concise condition tags (comma separated)", nullable=True)
    description: Optional[str] = Field(default=None, description="Long description of the forecast", nullable=True)
    icon: Optional[str] = Field(default=None, description="Icon identifier for the weather", nullable=True)

    stations: Optional[str] = Field(default=None, description="Station identifiers (comma separated)", nullable=True)
    source: Optional[str] = Field(default=None, description="Data source identifier", nullable=True)

    hours: Optional[str] = Field(default=None, description="Hourly data as JSON string", nullable=True)
