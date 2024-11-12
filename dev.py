from datetime import datetime, timedelta
from src.flights.models.models import Airport, SearchItem
from src.flights.scrapers.scrapers import OneWayScraper

search_item = SearchItem(
    origin=Airport("FCO"),
    destination=Airport("MUC"),
    departure_date=datetime.today().date() + timedelta(days=10),
    direct_only=True,
)
scraper = OneWayScraper(search_item)

print(scraper)

print(scraper.url)
