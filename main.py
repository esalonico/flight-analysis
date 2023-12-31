from datetime import date

from flight_analysis.airport import Airport
from flight_analysis.scraper import DirectOneWayScraper
from flight_analysis.search_query import SearchQuery
from flight_analysis.sheets.download_sheets import download_all_sheets

if __name__ == "__main__":
    download_all_sheets()
    
    a1 = Airport("MUC")
    a2 = Airport("MAD")
    print(a1.__repr__()); print(a2.__repr__())

    datetime_dep = date(2024, 3, 25) # 25th March 2024
    sq = SearchQuery(a1, a2, datetime_dep)
    print(sq)
    
    s = DirectOneWayScraper(sq)
    print(s)
    
    s.scrape()
    flights = s.flights
    for f in flights:
        print(f)