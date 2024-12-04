from datetime import datetime, timedelta

from src.flights.models.models import Airport, SingleSearch
from src.flights.scrapers.scrapers import OneWayScraper

search_item = SingleSearch(
    origin=Airport("IST"),
    destination=Airport("ESB"),
    departure_date=datetime.today().date() + timedelta(days=10),
    direct_only=True,
)
scraper = OneWayScraper(search_item)

scraper.get_raw_flight_results()
