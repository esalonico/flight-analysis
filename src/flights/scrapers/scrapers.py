from typing import List, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebElement
from webdriver_manager.chrome import ChromeDriverManager

import src.flights.utils.utils as utils
from src.flights.models.models import CompositeSearch, Flight, SingleSearch
from src.flights.scrapers import utils as scraper_utils

TIMEOUT = 15  # seconds


class BaseScraper:
    def __init__(self, composite_search: CompositeSearch) -> None:
        self.composite_search = composite_search

        self.driver = self._create_driver()

    def __del__(self):
        if hasattr(self, "driver") and self.driver:
            self.driver.save_screenshot("debug/screenshot.png")  # TODO: delete line
            self.driver.quit()

    def _create_driver(self) -> webdriver.Chrome:
        """
        Creates a Chrome webdriver instance.

        :return: Chrome webdriver instance.
        """
        options = Options()
        options.add_argument("--no-sandbox")
        # options.add_argument("--headless")
        options.add_argument("--window-size=1200, 2000")

        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def build_single_search_url(self, single_search_obj: SingleSearch) -> str:
        raise NotImplementedError("Method must be implemented in subclass.")


class OneWayScraper(BaseScraper):
    def build_single_search_url(self, single_search_obj: SingleSearch) -> str:
        """
        Build the URL for the flight search of a single search item.

        :return: URL string for the single item flight search
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{single_search_obj.destination.iata}%20Airport"
        url += f"%20from%20{single_search_obj.origin.iata}"

        if single_search_obj.direct_only:
            return f"{url}%20on%20{single_search_obj.departure_date}%20oneway%20direct&curr=EUR&gl=IT"

        return f"{url}%20on%20{single_search_obj.departure_date}%20oneway&curr=EUR&gl=IT"

    def scrape_all_flights(self) -> Optional[List[Flight]]:
        """
        Scrape all flights for the given search items.
        Combines all flights from all search items into a single list.

        :return: List of Flight objects.
        """
        all_flights = []
        for single_search_obj in self.composite_search.single_searches:
            flights = self.get_flights_objects(single_search_obj)
            all_flights.extend(flights)

        return all_flights

    def get_flights_objects(self, single_search_obj: SingleSearch) -> Optional[List[Flight]]:
        """
        Get a list of Flight objects from a single search item.
        If no direct flights are found, an empty list is returned.

        :return: List of Flight objects.
        """

        def is_li_element_view_more_flights(element: WebElement) -> bool:
            """
            Checks if a <li> WebElement is a "View more flights" element (menu to load more flights).

            :param element: <li> WebElement to check.
            :return: True if the element is a "View more flights" element, False otherwise.
            """
            try:
                return bool(element.find_element(By.CSS_SELECTOR, ".zISZ5c.QB2Jof"))
            except NoSuchElementException:
                return False

        # build and open the URL
        url = self.build_single_search_url(single_search_obj)
        self.driver.get(url)

        # handle google terms and conditions page
        if scraper_utils.is_google_terms_and_conditions_page(self.driver.page_source):
            scraper_utils.skip_google_terms_and_conditions_page(self.driver, TIMEOUT)

        # check if there are nonstop flights available
        if scraper_utils.no_nonstop_flights_found(self.driver, TIMEOUT):
            return []

        # click on "cheapest" tab
        scraper_utils.click_on_cheapest_tab(self.driver, TIMEOUT)

        # wait for the actual cheapest prices to load
        scraper_utils.wait_for_cheapest_prices_to_load(self.driver, TIMEOUT)

        # get the HTML section containing flight data
        flights_sections = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)

        # TODO: delete
        # save element as screenshot
        for i, s in enumerate(flights_sections):
            s.screenshot(f"debug/flights_section_{i+1}.png")
        print(self.driver.current_url)

        # extract flight data from HTML sections and return as a list of Flight objects
        flights = []
        for section in flights_sections:
            for row in section.find_elements(By.TAG_NAME, "li"):
                # skip "View more flights" elements
                if is_li_element_view_more_flights(row):
                    continue
                flight = scraper_utils.extract_flight_data_from_li(row, dep_date=single_search_obj.departure_date)
                flights.append(flight)

        return flights

    def make_flights_dataframe(self, flights: List[Flight]) -> pd.DataFrame:
        """
        Generate a pandas DataFrame from a list of Flight objects.

        :param flights: List of Flight objects.
        :return: pandas DataFrame.
        """
        df = pd.DataFrame([f.model_dump() for f in flights])
        df["origin"] = df.origin.apply(lambda x: x.get("iata"))
        df["destination"] = df.destination.apply(lambda x: x.get("iata"))

        # sort
        df = df.sort_values(by=["price", "n_stops", "flight_time", "dep_datetime"], ascending=[True, True, True, True])

        # move columns to the front
        df = utils.move_column_to_position(df, "airline_logo_url", 0)

        # reset index
        df = df.reset_index(drop=True)

        # export to csv
        df.to_csv("debug/flights.csv", index=False)

        return df
