from datetime import date, datetime

from backend.itineraries.itinerary import OneWayItinerary, RoundTripItinerary
from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SimpleSearchQuery, MultiDateSearchQuery
from backend.utils.sheets_downloader import download_all_sheets

if __name__ == "__main__":
    timestamp = datetime.now()

    download_all_sheets(force_download=False)

    ### USER INPUTS
    airport_from = Airport("FCO")
    airport_to = Airport("LIS")
    datetime_dep = date(2024, 6, 25)
    datetime_ret = date(2024, 7, 2)
    ###

    search_query = SimpleSearchQuery(airport_from, airport_to, datetime_dep, return_date=datetime_ret)

    # ### USER INPUTS
    # airport_from = Airport("FCO")
    # airport_to = Airport("LIS")
    # datetime_dep = [date(2024, 6, 25), date(2024, 6, 26)]
    # datetime_ret = [date(2024, 7, 2), date(2024, 7, 3)]
    # ###

    # search_query = MultiDateSearchQuery(airport_from, airport_to, datetime_dep)
    # print(search_query)

    # Case 1: Direct one-way
    # direct_one_way_itinerary = OneWayItinerary(search_query, direct_only=True)
    # direct_one_way_itinerary.scrape()
    # print(direct_one_way_itinerary.df)

    # Case 2: Layover one-way
    # layover_one_way_itinerary = OneWayItinerary(search_query, direct_only=False)
    # layover_one_way_itinerary.scrape()
    # print(layover_one_way_itinerary.df)

    # Case 3: Direct round-trip
    # direct_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=True)
    # direct_round_trip_itinerary.scrape()
    # print(direct_round_trip_itinerary.df)

    # Case 4: Layover round-trip
    layover_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=False)
    layover_round_trip_itinerary.scrape()
    print(layover_round_trip_itinerary.df)
