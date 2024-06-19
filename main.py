from datetime import date, datetime

from backend.itineraries.itinerary import OneWayItinerary, RoundTripItinerary
from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SearchQuery
from backend.utils.sheets_downloader import download_all_sheets

if __name__ == "__main__":
    timestamp = datetime.now()

    ### USER INPUTS
    airport_from = Airport("FCO")
    airport_to = Airport("LIS")
    datetime_dep = date(2024, 6, 25)
    datetime_ret = date(2024, 7, 2)
    ###

    download_all_sheets()

    search_query = SearchQuery(airport_from, airport_to, datetime_dep, return_date=datetime_ret)
    print(search_query)

    # Case 1: Direct one-way
    direct_one_way_scraper = OneWayScraper(search_query, direct_only=True)
    direct_one_way_scraper.scrape()
    direct_one_way_itinerary = OneWayItinerary(direct_one_way_scraper)
    direct_one_way_itinerary_df = direct_one_way_itinerary.make_itinerary_df(export_to="export/direct_one_way_itinerary.csv")
    print(direct_one_way_itinerary_df)

    # Case 2: Layover one-way
    # layover_one_way_scraper = OneWayScraper(search_query, direct_only=False)
    # layover_one_way_scraper.scrape()
    # layover_one_way_itinerary = OneWayItinerary(layover_one_way_scraper)
    # layover_one_way_itinerary_df = layover_one_way_itinerary.make_itinerary_df(export_to="export/layover_one_way_itinerary.csv")
    # print(layover_one_way_itinerary_df)

    # Case 3: Direct round-trip
    # direct_round_trip_scraper = RoundTripScraper(search_query, direct_only=True)
    # direct_round_trip_scraper.scrape()
    # direct_round_trip_itinerary = RoundTripItinerary(direct_round_trip_scraper)
    # direct_round_trip_itinerary_df = direct_round_trip_itinerary.make_itinerary_df(export_to="export/direct_round_trip_itinerary.csv")
    # print(direct_round_trip_itinerary_df)

    # Case 4: Layover round-trip
    # layover_round_trip_scraper = RoundTripScraper(search_query, direct_only=False)
    # layover_round_trip_scraper.scrape()
    # layover_round_trip_itinerary = RoundTripItinerary(layover_round_trip_scraper)
    # layover_round_trip_itinerary_df = layover_round_trip_itinerary.make_itinerary_df(export_to="export/layover_round_trip_itinerary.csv")
    # print(layover_round_trip_itinerary_df)
