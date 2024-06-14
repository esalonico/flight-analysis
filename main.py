from datetime import date

from flight_analysis.objects.airport import Airport
from flight_analysis.scrapers.direct_one_way_scraper import DirectOneWayScraper
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils.sheets_downloader import download_all_sheets

if __name__ == "__main__":
    ### USER INPUTS
    airport_from = Airport("FCO")
    airport_to = Airport("AUH")
    datetime_dep = date(2024, 6, 25)
    ###

    download_all_sheets()

    search_query = SearchQuery(airport_from, airport_to, datetime_dep)
    print(search_query)

    direct_one_way_scraper = DirectOneWayScraper(search_query)
    print(direct_one_way_scraper)

    direct_one_way_scraper.scrape()
    flights_df = direct_one_way_scraper.make_flights_df()
    print(flights_df)
    
    print(direct_one_way_scraper.metadata)
