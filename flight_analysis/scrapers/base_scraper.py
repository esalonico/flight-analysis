import re

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from flight_analysis.objects.flight import Flight
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils import utils
from datetime import datetime


class BaseScraper:
    def __init__(self, search_query: SearchQuery, datetime_access: datetime = datetime.now()):
        self.search_query = search_query
        self.datetime_access = datetime_access
        self.flights = None
        self.metadata = None  # metadata of the current scrape

        self.driver = self._create_driver()
        self.url = self._build_url()

    def __repr__(self) -> str:
        return f"Scrape({self.__class__.__name__}, {self.url})"

    def _create_driver(self) -> webdriver.Chrome:
        """
        Creates a Chrome webdriver instance.
        Returns: Chrome webdriver.
        """
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        return driver

    def _build_url() -> str:
        raise NotImplementedError("This method must be implemented in a subclass.")

    def scrape(self) -> None:
        raise NotImplementedError("This method must be implemented in a subclass.")

    def _skip_google_terms_page(self, driver: webdriver.Chrome, timeout: int = 15) -> None:
        """
        Returns True if the page html represent Google's Terms and Conditions page.
        """
        if "Before you continue to Google" not in driver.page_source:
            return

        # click on accept terms button
        WebDriverWait(driver, timeout).until(lambda s: "Before you continue to Google" in s.page_source)

        # click on accept terms button
        WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept all')]"))
        ).click()

    def _get_raw_flight_results(self) -> list:
        """
        Extracts the raw flight results from the page.
        """
        return self.driver.find_element(by=By.XPATH, value='//body[@id = "yDmH0d"]').text.split("\n")

    def _search_has_no_flights(self, results_raw: list) -> bool:
        """
        Returns True if the search has no flights.
        """
        return len([a for a in results_raw if "No nonstop flights found" in a]) > 0

    def _filter_raw_results(self, results_raw: list) -> list:
        """
        Filters the raw results to only include the flights-related elements of the list.
        This is done by identifying the start and end index of the flights-related elements.
        Returns: Cleaned list of flights-related elements.
        """
        # case: no flights found for that search --> return empty list
        if self._search_has_no_flights(results_raw):
            return []

        # TODO: there is a string here that says "X results found.". Maybe use it to double check later steps.
        start_regex = re.compile("Sort by:")
        end_regex = re.compile("Language")

        start_idx = [i for i, el in enumerate(results_raw) if start_regex.match(el)][0] + 1
        end_idx = [i for i, el in enumerate(results_raw) if end_regex.match(el)][0]

        filtered = results_raw[start_idx:end_idx]

        # fix encoding
        filtered = [x.encode("ascii", "ignore").decode() for x in filtered]

        # remove empty elements
        filtered = [x for x in filtered if x not in ["", " ", "  "]]

        # remove some clutter words (supports regex)
        words_to_remove = [
            "Price insights",
            "Prices are currently",
            "View price history",
            "Other flights",
            "Avoids as much CO2",
            "The cheapest time to book",
        ]

        # compile all regexes
        regexes = [re.compile(r) for r in words_to_remove]
        filtered = [x for x in filtered if not any(regex.match(x) for regex in regexes)]

        return filtered

    def _get_flight_search_metadata(self, results_raw: list) -> dict:
        """
        Extracts the flight search metadata from the raw results.
        Returns: Flight search metadata as dict.
        """
        metadata = dict()

        # case no flights found for that search --> return empty dict
        if self._search_has_no_flights(results_raw):
            return metadata

        # get number of results returned
        n_flights_regex = re.compile(r"(\d+) result(s)? returned")
        try:
            n_flights = int([x for x in results_raw if n_flights_regex.match(x)][0].split(" ")[0])
        except IndexError:
            n_flights = "unknown"
        metadata["n_flights"] = n_flights

        # price trend
        price_trend_regex = re.compile(r"Prices are currently")
        price_trend = [x for x in results_raw if price_trend_regex.match(x)]
        if len(price_trend) == 1:
            metadata["price_trend"] = price_trend[0]
        else:
            metadata["price_trend"] = None

        return metadata

    def _split_raw_results_into_flights(self, results_raw: list) -> list:
        """
        Splits the raw results into individual flights.
        Returns: List of flights (list of lists)
        """
        # case: no flights found for that search --> return empty list
        if not results_raw:
            return []

        # remove clutter strings
        clutter_strings = [
            "round trip",
            "Other departing flights",
            "Separate tickets booked together",
            "Price graph",
            "Date grid",
        ]

        for clutter in clutter_strings:
            results_raw = [x for x in results_raw if clutter.lower() not in x.lower()]

        time_pattern = re.compile(r"(1[0-2]|0?[1-9]):([0-5][0-9])([APap][Mm])")  # 12:30PM, 11:40AM etc...

        # get the indices where each flight starts
        flight_start_idxs = [i for i, el in enumerate(results_raw) if time_pattern.match(el)]
        flight_start_idxs = [i for i in flight_start_idxs if i + 1 in flight_start_idxs]

        # split the raw list into single flights
        flights = []
        for i in range(len(flight_start_idxs)):
            if i == len(flight_start_idxs) - 1:
                flights.append(results_raw[flight_start_idxs[i] :])
            else:
                flights.append(results_raw[flight_start_idxs[i] : flight_start_idxs[i + 1]])

        return flights

    def _clean_flight_details(self, flight_list: list, sq: SearchQuery) -> dict:
        """
        From a list of strings (representing a flight), return a dictionary of flights details after cleaning.
        :param flight_list: List of strings representing a flight.
        :param sq: SearchQuery object.
        :return: Dictionary of flight details.
        """
        flight_dict = dict()

        flight_dict["airport_dep"] = self.get_airports_from_txt(flight_list[4])[0]
        flight_dict["airport_arr"] = self.get_airports_from_txt(flight_list[4])[1]
        flight_dict["time_dep"] = flight_list[0]

        if self.is_flight_returning_flight(sq, flight_dict["airport_dep"]):
            flight_date = sq.return_date
        else:
            flight_date = sq.departure_date

        flight_dict["datetime_dep"] = utils.convert_string_date_time_to_datetime(flight_date, flight_list[0])
        flight_dict["datetime_arr"] = utils.convert_string_date_time_to_datetime(flight_date, flight_list[1])

        flight_dict["airline"] = utils.format_airline_correctly(flight_list[2])
        flight_dict["duration"] = utils.convert_string_to_duration(flight_list[3])
        flight_dict["price"] = int(flight_list[-1].replace(",", ""))

        return flight_dict

    def is_flight_returning_flight(self, sq: SearchQuery, airport_dep: str) -> bool:
        """
        If the departure airport of the search query is equal to the departure airport scraped, then it is a departing flight, otherwise it is a returning flight.
        """
        if sq.airport_dep.iata == airport_dep:
            return False
        return True

    def get_airports_from_txt(self, txt: str) -> tuple:
        """
        Extracts the departure and arrival airports from a string.
        :param txt: String containing the 2 airports (ex. FCOMAD, MUCFCO).
        :return: Tuple of airport codes.
        """
        pattern = re.compile(r"^[A-Z]{6}$")

        if pattern.match(txt):
            airport1 = txt[:3]
            airport2 = txt[3:]
            return airport1, airport2
        return None, None

    def make_flight_objects_from_driver(self, flight_combination: int = None, url: str = None) -> list[Flight]:
        """
        Create Flight objects from a driver page.

        :return: List of Flight objects
        """
        results_raw = self._get_raw_flight_results()
        results_raw_filtered = self._filter_raw_results(results_raw)

        self.metadata = self._get_flight_search_metadata(results_raw)

        flights = self._split_raw_results_into_flights(results_raw_filtered)

        flight_objects = []
        for flight_list in flights:
            flight_dict = self._clean_flight_details(flight_list, self.search_query)
            flight_dict["flight_combination"] = flight_combination
            flight_dict["url"] = url
            flight_obj = Flight(self.search_query, flight_dict, datetime_access=self.datetime_access)

            flight_objects.append(flight_obj)

        return flight_objects

    def make_flights_df(self) -> pd.DataFrame:
        """
        Returns a DataFrame of the flights.
        """
        if self.flights is None or len(self.flights) == 0:
            return pd.DataFrame()

        # make dataframe from list of dictionaries
        flights_data = [f.to_dict() for f in self.flights]
        flights_df = pd.DataFrame(flights_data)
        flights_df = flights_df.drop_duplicates()

        return flights_df.reset_index(drop=True)
