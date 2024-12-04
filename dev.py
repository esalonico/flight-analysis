from datetime import datetime, timedelta

from src.flights.models.models import Airport, SingleSearch
from src.flights.scrapers.scrapers import OneWayScraper
import pandas as pd

search_item = SingleSearch(
    origin=Airport("IST"),
    destination=Airport("ESB"),
    departure_date=datetime.today().date() + timedelta(days=10),
    direct_only=True,
)
scraper = OneWayScraper(search_item)

flights = scraper.get_flights_objects()

df = pd.DataFrame([f.model_dump() for f in flights])
print(df)
