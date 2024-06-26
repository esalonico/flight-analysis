from datetime import date, datetime

from backend.itineraries.itinerary import (CrazyLayoverItinerary,
                                           OneWayItinerary, RoundTripItinerary)
from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SearchQuery
from backend.utils.sheets_downloader import download_all_sheets

if __name__ == "__main__":
    timestamp = datetime.now()

    download_all_sheets(force_download=False)

    # SearchQuery
    airports_dep = [Airport("FCO")]
    airports_arr = [Airport("FMM")]
    departure_dates = [date(2024, 7, 25), date(2024, 7, 26)]
    return_dates = [date(2024, 7, 29)]
    search_query = SearchQuery(airports_dep, airports_arr, departure_dates, return_dates=None, debug=False)

    itinerary = CrazyLayoverItinerary(search_query, direct_only=False, max_stops=2)
    itinerary.scrape()
    # x = airports_dep[0].get_all_connections("FMM", 2)
    # print(x)

    # Case 1: Direct one-way
    # direct_one_way_itinerary = OneWayItinerary(search_query, direct_only=True)
    # direct_one_way_itinerary.scrape(export_to="miani.csv")
    # print(direct_one_way_itinerary.df)

    # Case 2: Layover one-way
    # layover_one_way_itinerary = OneWayItinerary(search_query, direct_only=False)
    # layover_one_way_itinerary.scrape(export_to="miani.csv")
    # print(layover_one_way_itinerary.df)

    # Case 3: Direct round-trip
    # direct_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=True)
    # direct_round_trip_itinerary.scrape(export_to="mianivacca.csv")
    # print(direct_round_trip_itinerary.df)

    # Case 4: Layover round-trip
    # layover_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=False)
    # layover_round_trip_itinerary.scrape(export_to="mianiputtana.csv")
    # print(layover_round_trip_itinerary.df)
