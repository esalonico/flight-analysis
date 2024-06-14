from flight_analysis.objects.flight import Flight
from flight_analysis.scrapers.base_scraper import BaseScraper
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils import utils


class DirectOneWayScraper(BaseScraper):
    def __init__(self, flight: Flight) -> None:
        super().__init__(flight)

    def _build_url(self, search_query: SearchQuery) -> str:
        """
        Builds the URL to scrape.
        :param search_query: SearchQuery object.
        :return: URL as string.
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{search_query.airport_arr.iata}"
        url += f"%20from%20{search_query.airport_dep.iata}"
        url += f"%20on%20{search_query.departure_date}%20oneway%20direct&curr=EUR&gl=IT"

        return url

    def _clean_flight_details(self, flight_list: list, sq: SearchQuery) -> dict:
        """
        From a list of strings (representing a flight), return a dictionary of flights details after cleaning.
        :param flight_list: List of strings representing a flight.
        :param sq: SearchQuery object.
        :return: Dictionary of flight details.
        """
        flight_dict = dict()

        flight_dict["time_dep"] = flight_list[0]
        flight_dict["datetime_dep"] = utils.convert_string_date_time_to_datetime(sq.departure_date, flight_list[0])
        flight_dict["datetime_arr"] = utils.convert_string_date_time_to_datetime(sq.departure_date, flight_list[1])
        flight_dict["airline"] = utils.format_airline_correctly(flight_list[2])
        flight_dict["duration"] = utils.convert_string_to_duration(flight_list[3])
        flight_dict["price"] = int(flight_list[-1].replace(",", ""))

        return flight_dict

    def scrape(self):
        """
        Scrapes the flight results page.
        Updates the flights attribute of the class.
        """
        results_raw = super()._get_raw_flight_results(self.driver, self.url)
        results_raw_filtered = super()._filter_raw_results(results_raw)

        self.metadata = super()._get_flight_search_metadata(results_raw)

        flights = super()._split_raw_results_into_flights(results_raw_filtered)

        flight_objects = []
        for flight_list in flights:
            flight_dict = self._clean_flight_details(flight_list, self.search_query)
            flight_obj = Flight(self.search_query, flight_dict)

            flight_objects.append(flight_obj)

        # close the driver
        self.driver.quit()

        # save flight objects to class
        self.flights = flight_objects
