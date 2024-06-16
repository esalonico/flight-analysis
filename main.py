from datetime import date, datetime

from flight_analysis.itineraries.itinerary import DirectOneWayItinerary, DirectReturnItinerary
from flight_analysis.objects.airport import Airport
from flight_analysis.scrapers.direct_one_way_scraper import DirectOneWayScraper
from flight_analysis.scrapers.direct_return_scraper import DirectReturnScraper
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils.sheets_downloader import download_all_sheets

if __name__ == "__main__":
    timestamp = datetime.now()

    ### USER INPUTS
    airport_from = Airport("FMM")
    airport_to = Airport("BGY")
    datetime_dep = date(2024, 6, 25)
    datetime_ret = date(2024, 7, 2)
    ###

    download_all_sheets()

    search_query = SearchQuery(airport_from, airport_to, datetime_dep, return_date=datetime_ret)
    print(search_query)

    # direct_one_way_scraper = DirectOneWayScraper(search_query)
    # print(direct_one_way_scraper)
    # direct_one_way_scraper.scrape()
    # itinerary = DirectOneWayItinerary(direct_one_way_scraper)
    # itinerary_df = itinerary.make_itinerary_df()
    # print(itinerary_df)

    direct_return_scraper = DirectReturnScraper(search_query)
    direct_return_scraper.scrape()

    itinerary = DirectReturnItinerary(direct_return_scraper)
    itinerary_df = itinerary.make_itinerary_df()

    if itinerary_df:
        itinerary_df.to_csv("itinerary.csv", index=False)

    print(itinerary_df)
