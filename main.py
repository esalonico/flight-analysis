from datetime import date, datetime

from flight_analysis.itineraries.itinerary import DirectOneWayItinerary, ReturnItinerary, LayoverOneWayItinerary
# TODO: adapt onewayitinerary e scraper into one class
from flight_analysis.objects.airport import Airport
from flight_analysis.scrapers.direct_one_way_scraper import DirectOneWayScraper
from flight_analysis.scrapers.layover_one_way_scraper import LayoverOneWayScaper
from flight_analysis.scrapers.return_scraper import ReturnScraper
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils.sheets_downloader import download_all_sheets

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

    # direct_one_way_scraper = DirectOneWayScraper(search_query)
    # print(direct_one_way_scraper)
    # direct_one_way_scraper.scrape()
    # itinerary = DirectOneWayItinerary(direct_one_way_scraper)
    # itinerary_df = itinerary.make_itinerary_df()
    # print(itinerary_df)

    # TODO: adapt
    # direct_return_scraper = ReturnScraper(search_query)
    # direct_return_scraper.scrape()
    # itinerary = ReturnItinerary(direct_return_scraper)
    # itinerary_df = itinerary.make_itinerary_df()
    # if itinerary_df:
    #     itinerary_df.to_csv("itinerary.csv", index=False)
    # print(itinerary_df)

    # layover_one_way_scraper = LayoverOneWayScaper(search_query)
    # print(layover_one_way_scraper)
    # layover_one_way_scraper.scrape()
    # itinerary = LayoverOneWayItinerary(layover_one_way_scraper)
    # itinerary_df = itinerary.make_itinerary_df()
    # itinerary_df.to_csv("itinerary.csv", index=False)
    # print(itinerary_df)

    layover_return_scraper = ReturnScraper(search_query, direct_only=False)
    print(layover_return_scraper.url)

    layover_return_scraper.scrape()
    itinerary = ReturnItinerary(layover_return_scraper)
    itinerary_df = itinerary.make_itinerary_df()
    itinerary_df.to_csv("itinerary.csv", index=False)
    print(itinerary_df)
