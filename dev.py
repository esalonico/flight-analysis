from datetime import datetime, timedelta
from src.flights.models.models import Airport, SingleSearch
from src.flights.scrapers.scrapers import OneWayScraper

search_item = SingleSearch(
    origin=Airport("FCO"),
    destination=Airport("MUC"),
    departure_date=datetime.today().date() + timedelta(days=10),
    direct_only=True,
)
scraper = OneWayScraper(search_item)

print(scraper)

print(scraper.url)
