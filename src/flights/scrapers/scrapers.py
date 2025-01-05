import os
import time
from datetime import date
from typing import List, Optional

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebElement
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

import src.flights.utils.utils as utils
from src.flights.models.models import CompositeSearch, Flight, ReturnFlight, SingleSearch
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
        except StaleElementReferenceException as e:
            print("STALEEEEEEEE")
            raise e


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
        li_elements = scraper_utils.find_all_li_elements_in_section(self.driver, section, TIMEOUT)

        # iterate over each <li> element and extract flight data
        for li_element in li_elements:
            # skip the "View more flights" element (end of the list)
            if self._is_view_more_flights_element(li_element):
                continue

            flight = scraper_utils.extract_flight_data_from_li(self.driver, li_element, dep_date=departure_date, url=self.driver.current_url)

            if flight:
                flights.append(flight)

        return flights

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
        if not os.path.exists("debug"):
            os.makedirs("debug")
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
        # for single_search_obj in tqdm(self.composite_search.single_searches):
        for single_search_obj in self.composite_search.single_searches:
            self.set_up_scraping(single_search_obj)
            flights = self.scrape_return_flights(single_search_obj)
            all_flights.extend(flights)
            break

        return all_flights

    def set_up_scraping(self, single_search_obj: SingleSearch) -> None:
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

    def scrape_return_flights(self, single_search_obj: SingleSearch) -> Optional[List[ReturnFlight]]:
        """ """
        all_return_flights = []

        # for each departing flight section
        # TODO: enable multiple sections
        dep_sections = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)
        # dep_sections = [scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)[0]]
        
        for dep_sect_idx in range(len(dep_sections)):
            print(f"Departing section {dep_sect_idx}/{len(dep_sections)}")
            dep_section = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)[dep_sect_idx]
            dep_li_elements = scraper_utils.find_all_li_elements_in_section(self.driver, dep_section, TIMEOUT)

            # for each departing flight
            for dep_li_elem_idx in range(len(dep_li_elements)):
                print(f"Departing flight {dep_li_elem_idx}/{len(dep_li_elements)}")
                dep_section = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)[dep_sect_idx]
                dep_li_elem = scraper_utils.find_all_li_elements_in_section(self.driver, dep_section, TIMEOUT)[dep_li_elem_idx]

                # create departing flight object
                dep_flight = scraper_utils.extract_flight_data_from_li(
                    self.driver,
                    dep_li_elem,
                    dep_date=single_search_obj.departure_date,
                    url=self.driver.current_url,
                )
                if not dep_flight:
                    continue

                # click on the departing flight to see the return flights
                dep_li_elem.click()
                # wait for the return sections to reload
                scraper_utils.wait_until_all_lis_loaded(self.driver, TIMEOUT)

                # for each returning flight section
                ret_sections = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)
                for ret_sect_idx in range(len(ret_sections)):
                    print(f"Returning section {ret_sect_idx}/{len(ret_sections)}")
                    ret_section = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)[ret_sect_idx]
                    ret_li_elements = scraper_utils.find_all_li_elements_in_section(self.driver, ret_section, TIMEOUT)

                    # for each returning flight
                    for ret_li_elem_idx in range(len(ret_li_elements)):
                        print(f"Returning flight {ret_li_elem_idx}/{len(ret_li_elements)}")
                        ret_section = scraper_utils.get_html_sections_containing_flight_data(self.driver, TIMEOUT)[ret_sect_idx]
                        ret_li_elem = scraper_utils.find_all_li_elements_in_section(self.driver, ret_section, TIMEOUT)
                        try:
                            ret_li_elem = ret_li_elem[ret_li_elem_idx]
                        except IndexError:
                            print("IndexError")
                            continue

                        # build return flight object
                        ret_flight = scraper_utils.extract_flight_data_from_li(
                            self.driver,
                            ret_li_elem,
                            dep_date=single_search_obj.return_date,
                            url=self.driver.current_url,
                        )

                        if not ret_flight:
                            continue

                        # create ReturnFlight object
                        full_return_flight = ReturnFlight(
                            origin=dep_flight.origin,
                            destination=dep_flight.destination,
                            dep_datetime=dep_flight.dep_datetime,
                            arr_datetime=dep_flight.arr_datetime,
                            airlines=dep_flight.airlines,
                            flight_time=dep_flight.flight_time,
                            n_stops=dep_flight.n_stops,
                            layover_location=dep_flight.layover_location,
                            layover_time=dep_flight.layover_time,
                            only_hand_luggage=dep_flight.only_hand_luggage,
                            airline_logo_url=dep_flight.airline_logo_url,
                            url=dep_flight.url,
                            return_origin=ret_flight.origin,
                            return_destination=ret_flight.destination,
                            return_dep_datetime=ret_flight.dep_datetime,
                            return_arr_datetime=ret_flight.arr_datetime,
                            return_airlines=ret_flight.airlines,
                            return_flight_time=ret_flight.flight_time,
                            return_n_stops=ret_flight.n_stops,
                            return_layover_location=ret_flight.layover_location,
                            return_layover_time=ret_flight.layover_time,
                            return_airline_logo_url=ret_flight.airline_logo_url,
                            return_url=ret_flight.url,
                            price=ret_flight.price,
                        )
                        print("Full return flight:", full_return_flight)
                        all_return_flights.append(full_return_flight)

                        # save pydantic model to json
                        with open(f"debug/returns/return_flight_{dep_sect_idx}_{dep_li_elem_idx}_{ret_sect_idx}_{ret_li_elem_idx}.json", "w") as f:
                            f.write(full_return_flight.model_dump_json())

                # go back to the departing flights page
                self.driver.back()
                self.driver.refresh()
                print("_____________________________")

        return all_return_flights
