from datetime import datetime, timedelta

from src.flights.models.models import Airport, CompositeSearch
from src.flights.scrapers.scrapers import ReturnScraper

composite_search = CompositeSearch(
    origins=[Airport("LHR"), Airport("LGW")],
    destinations=[Airport("CDG"), Airport("ORY")],
    departure_dates=[datetime.now().date() + timedelta(days=i) for i in range(1, 3)],
    return_dates=[datetime.now().date() + timedelta(days=i) for i in range(8, 10)],
    direct_only=False,
)


if __name__ == "__main__":
    scraper = ReturnScraper(composite_search)
    flights = scraper.scrape_all_searches()
    
    print(flights)
