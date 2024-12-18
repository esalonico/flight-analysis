from datetime import date
from typing import List, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebElement
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

import src.flights.utils.utils as utils
from src.flights.models.models import CompositeSearch, Flight, SingleSearch
from src.flights.scrapers import utils as scraper_utils

TIMEOUT = 15  # seconds


class BaseScraper:
    """
    Base class for flight scrapers.
    Initializes a Selenium Chrome driver and provides a common interface for derived scraper classes.
    """

    def __init__(self, composite_search: CompositeSearch) -> None:
        self.composite_search = composite_search
        self.driver = self._create_driver()

    def __del__(self):
        if hasattr(self, "driver") and self.driver:
            self.driver.quit()

    def _create_driver(self) -> webdriver.Chrome:
        """
        Create and return a configured Chrome WebDriver instance.

        :return: Chrome WebDriver instance.
        """
        options = Options()
        options.add_argument("--no-sandbox")
        # options.add_argument("--headless")
        options.add_argument("--window-size=1200,2000")

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.set_page_load_timeout(TIMEOUT)

        return driver

    def construct_search_url(self, single_search_obj: SingleSearch) -> str:
        """
        Construct the URL for a given single search object. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def _handle_google_terms_and_conditions_page(self) -> None:
        """
        If the Google terms and conditions page is displayed, handle it by clicking 'I agree' or the relevant button.
        """
        if scraper_utils.is_google_terms_and_conditions_page(self.driver.page_source):
            scraper_utils.skip_google_terms_and_conditions_page(self.driver, TIMEOUT)


class OneWayScraper(BaseScraper):
    """
    Scraper class for one-way flights searches, both direct and with layovers.
    """

    def construct_search_url(self, single_search_obj: SingleSearch) -> str:
        """
        Construct the URL for the given single search item.

        :param single_search_obj: SingleSearch object.
        :return: URL string.
        """
        base_url = "https://www.google.com/travel/flights"

        url = f"{base_url}?q=Flights%20to%20{single_search_obj.destination.iata}%20Airport%20from%20{single_search_obj.origin.iata}"

        direct_str = "%20direct" if single_search_obj.direct_only else ""
        url += f"%20on%20{single_search_obj.departure_date}%20oneway{direct_str}&curr=EUR&gl=IT"

        return url

    def fetch_flights_for_search(self, single_search_obj: SingleSearch) -> Optional[List[Flight]]:
        """
        Fetch flights for a single search scenario.

        :return: List of Flight objects or an empty list if no flights found.
        """
        # build and navigate to the search URL
        url = self.construct_search_url(single_search_obj)
        self.driver.get(url)

        # handle Google terms and conditions if present
        self._handle_google_terms_and_conditions_page()

        # check if any direct flights are available
        if scraper_utils.no_nonstop_flights_found(self.driver, TIMEOUT):
            print("No flights found for this search.")
            return []

        # click on "Cheapest" tab
        scraper_utils.click_on_cheapest_tab(self.driver, TIMEOUT)
        scraper_utils.wait_for_cheapest_prices_to_load(self.driver, TIMEOUT)

        # extract flight sections from the page
        flight_sections = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)
        if not flight_sections:
            print("No flight sections found.")
            return []

        # extract flights from each section
        flights = self._extract_flights_from_webelements_sections(flight_sections, single_search_obj.departure_date)

        return flights

    def scrape_all_searches(self) -> Optional[List[Flight]]:
        """
        Scrape flights for all SingleSearch objects in the CompositeSearch.

        :return: Combined list of Flight objects from all searches.
        """
        if not self.composite_search.single_searches:
            print("No single searches found in the composite search (list is empty).")
            return []

        all_flights = []
        for single_search_obj in tqdm(self.composite_search.single_searches):
            flights = self.fetch_flights_for_search(single_search_obj)
            if flights:
                all_flights.extend(flights)

        return all_flights

    def _extract_flights_from_webelements_sections(self, sections: List[WebElement], departure_date: date) -> List[Flight]:
        """
        Extract flight data from a list of WebElement sections.

        :param sections: List of WebElement sections containing flight data.
        :param departure_date: Departure date for the search.
        :return: List of Flight objects.
        """
        flights = []
        for section in sections:
            section_flights = self._parse_flight_section(section, departure_date)
            flights.extend(section_flights)

        return flights

    def _parse_flight_section(self, section: WebElement, departure_date: date) -> List[Flight]:
        """
        Parse a single flight section (list of <li> elements) for flight data.

        :param section: WebElement section containing flight data.
        :param departure_date: Departure date for the search.
        :return: List of Flight objects
        """
        flights = []
        # find all <li> elements in the section
        li_elements = section.find_elements(By.TAG_NAME, "li")

        # iterate over each <li> element and extract flight data
        for li_element in li_elements:
            # skip the "View more flights" element (end of the list)
            if self._is_view_more_flights_element(li_element):
                continue

            flight = scraper_utils.extract_flight_data_from_li(li_element, dep_date=departure_date, url=self.driver.current_url)

            if flight:
                flights.append(flight)

        return flights

    @staticmethod
    def _is_view_more_flights_element(element: WebElement) -> bool:
        """
        Check if a <li> WebElement is the "View more flights" element.

        :param element: WebElement to check.
        :return: True if the element is the "View more flights" element, False otherwise.
        """
        try:
            element.find_element(By.CSS_SELECTOR, ".zISZ5c.QB2Jof")
            return True
        except NoSuchElementException:
            return False

    def build_flights_dataframe(self, flights: List[Flight]) -> pd.DataFrame:
        """
        Convert a list of Flight objects into a pandas DataFrame with sorting and formatting.

        :param flights: List of Flight objects.
        :return: DataFrame with flight data.
        """
        if not flights:
            print("No flights to convert to DataFrame.")
            return pd.DataFrame()

        # build df and unnest origin and destination objects
        df = pd.DataFrame([f.model_dump() for f in flights])
        df["origin"] = df.origin.apply(lambda x: x.get("iata"))
        df["destination"] = df.destination.apply(lambda x: x.get("iata"))

        # sort and format columns
        df = df.sort_values(by=["price", "n_stops", "flight_time", "dep_datetime"], ascending=[True, True, True, True])
        df = utils.move_column_to_position(df, "airline_logo_url", 0)
        df.reset_index(drop=True, inplace=True)

        # TODO: remove: save to CSV for debugging
        df.to_csv("debug/flights.csv", index=False)

        return df


class ReturnScraper(BaseScraper):
    """
    Scraper class for return flights searches, both direct and with layovers.
    """

    def construct_search_url(self, single_search_obj: SingleSearch) -> str:
        """
        Construct the URL for the given single search item.

        :param single_search_obj: SingleSearch object.
        :return: URL string.
        """
        base_url = "https://www.google.com/travel/flights"

        url = f"{base_url}?q=Flights%20to%20{single_search_obj.destination.iata}%20Airport%20from%20{single_search_obj.origin.iata}"

        direct_str = "%20direct" if single_search_obj.direct_only else ""
        url += f"%20from%20{single_search_obj.departure_date}%20to%20{single_search_obj.return_date}%20return{direct_str}&curr=EUR&gl=IT"

        return url


    def scrape_all_searches(self) -> Optional[List[Flight]]:
        """
        Scrape flights for all SingleSearch objects in the CompositeSearch.

        :return: Combined list of Flight objects from all searches.
        """
        if not self.composite_search.single_searches:
            print("No single searches found in the composite search (list is empty).")
            return []

        all_flights = []
        for single_search_obj in tqdm(self.composite_search.single_searches):
            print(single_search_obj)
            break
            # TODO: CONTINUE

        return all_flights