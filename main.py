import logging
import os
from datetime import date, datetime, timedelta

from backend.itineraries.itinerary import CrazyLayoverItinerary, OneWayItinerary, RoundTripItinerary
from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SearchQuery
from backend.utils.sheets_downloader import download_all_sheets
from backend.utils.utils import setup_logging

if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(os.path.basename(__file__))

    timestamp = datetime.now()

    download_all_sheets(force_download=False)

    # SearchQuery
    # airports_dep = [Airport("FMM"), Airport("MUC")]
    # airports_arr = [Airport("SCQ"), Airport("LCG")]
    # departure_dates = [date(2024, 8, 22), date(2024, 8, 23)]
    # return_dates = [date(2024, 8, 25)]
    # search_query = SearchQuery(airports_dep, airports_arr, departure_dates, return_dates)

    # Case 1: Direct one-way
    # airports_dep = [Airport("IST")]
    # airports_arr = [Airport("TAS")]
    # departure_dates = [date(2024, 9, 17)]
    # search_query = SearchQuery(airports_dep, airports_arr, departure_dates)
    # direct_one_way_itinerary = OneWayItinerary(search_query, direct_only=True)
    # direct_one_way_itinerary.scrape(export_to="miani.csv")
    # print(direct_one_way_itinerary.df)

    # Case 2: Layover one-way
    # layover_one_way_itinerary = OneWayItinerary(search_query, direct_only=False)
    # layover_one_way_itinerary.scrape(export_to="miani.csv")
    # print(layover_one_way_itinerary.df)

    # Case 3: Direct round-trip
    # direct_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=True)
    # direct_round_trip_itinerary.scrape(export_to="miani.csv")
    # print(direct_round_trip_itinerary.df)

    # Case 4: Layover round-trip
    # layover_round_trip_itinerary = RoundTripItinerary(search_query, direct_only=False)
    # layover_round_trip_itinerary.scrape(export_to="galicia_roundtrip_layover.csv")
    # print(layover_round_trip_itinerary.df)

    # Case 5: Crazy layover
    airports_dep = [Airport("SCQ")]
    airports_arr = [Airport("MUC"), Airport("FMM")]

    departure_dates = [date(2024, 8, 25), date(2024, 8, 26)]

    search_query = SearchQuery(airports_dep, airports_arr, departure_dates)
    itinerary = CrazyLayoverItinerary(
        search_query,
        direct_only=False,
        max_stops=2,
        min_layover_time=timedelta(hours=1, minutes=30),
        max_layover_time=timedelta(hours=23),
    )
    itinerary.scrape(num_processes=20, export_to="galicia_ritorno.csv")
