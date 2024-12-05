from datetime import datetime, timedelta

from src.flights.models.models import Airport, SingleSearch
from src.flights.scrapers.scrapers import OneWayScraper
import pandas as pd

search_item = SingleSearch(
    origin=Airport("JFK"),
    destination=Airport("FCO"),
    departure_date=datetime.today().date() + timedelta(days=10),
    direct_only=True,
)

scraper = OneWayScraper(search_item)

flights = scraper.get_flights_objects()

df = scraper.make_flights_dataframe(flights)
df.to_csv("flights.csv", index=False)
print(df)
