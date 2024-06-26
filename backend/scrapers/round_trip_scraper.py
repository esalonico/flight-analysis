import re

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from backend.scrapers.base_scraper import BaseScraper
from backend.scrapers.search_query import SearchQuery


class RoundTripScraper(BaseScraper):
    def __init__(self, search_query: SearchQuery, direct_only: bool):
        """
        Initialize the RoundTripScraper.

        :param search_query: SearchQuery object with the search parameters
        :param direct_only: Whether to search for direct flights only (True) or not (False)
        """
        assert search_query.return_date, "Return date must be provided for a roundtrip flight."

        self.direct_only = direct_only

        super().__init__(search_query)

    def _build_url(self) -> str:
        """
        Build the URL for the flight search.

        :return: URL string for the flight search
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{self.search_query.airport_arr.iata}"
        url += f"%20from%20{self.search_query.airport_dep.iata}"
        url += f"%20on%20{self.search_query.departure_date}%20through%20{self.search_query.return_date}"

        if self.direct_only:
            url += f"%20roundtrip%20direct&curr=EUR&gl=IT"
        else:
            url += f"%20roundtrip&curr=EUR&gl=IT"

        return url

    def _wait_for_text_on_page(self, text: str, timeout: int = 15):
        """
        Wait for a given text to be present anywhere on the page.

        :param text: The text to wait for on the page
        :param timeout: Maximum time to wait for the text (default is 15 seconds)
        """
        WebDriverWait(self.driver, timeout).until(lambda driver: text in self.driver.page_source)

    def _get_flight_lists(self):
        """
        Get the <ul> elements that contains the list of flights.

        Example of the <ul> element:
        - Element with title "Best departing flights"
        - Element with title "Other departing flights"

        :return: WebElement representing the <ul> with the flight list
        :raises ValueError: If no flight list is found
        """
        try:
            # basically wait until the footer is loaded
            self._wait_for_text_on_page("Currency")
        except Exception as e:
            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "ul")))

        ul_elements = self.driver.find_elements(By.TAG_NAME, "ul")
        ul_flights_list = [ul for ul in ul_elements if "€" in ul.get_attribute("outerHTML")]

        if not ul_flights_list:
            raise ValueError("No flights found.")

        return ul_flights_list

    def _get_jsaction_element(self, parent_element) -> str:
        """
        Get the first element with a matching jsaction attribute within a parent element.

        :param parent_element: WebElement to search within
        :return: WebElement with the matching jsaction attribute
        :raises ValueError: If no matching element is found
        """
        jsaction_elements = parent_element.find_elements(By.CSS_SELECTOR, "[jsaction]")
        jsaction_pattern = re.compile(r"^click:([a-zA-Z0-9]{6};)([a-zA-Z0-9]{6}:)")
        filtered_elements = [el for el in jsaction_elements if jsaction_pattern.match(el.get_attribute("jsaction"))]
        if not filtered_elements:
            raise ValueError("No flight found.")
        return filtered_elements[0]

    def scrape(self):
        """
        Perform the scraping process to find and interact with flight elements.
        """
        url = self._build_url()
        self.driver.get(url)

        # Deal with Google's terms and conditions page
        self._skip_google_terms_page(self.driver, 15)

        # get list of departing flights
        try:
            ul_sections_containig_flights = self._get_flight_lists()
        except ValueError:
            print("No flights found.")
            self.driver.quit()
            return

        # Within the ul element, get all the li elements
        departing_flight_elements_li = []
        for section in ul_sections_containig_flights:
            flights_li_within_section = section.find_elements(By.TAG_NAME, "li")
            departing_flight_elements_li.extend(flights_li_within_section)

        n_departing_flights = len(departing_flight_elements_li)
        # print(f"{n_departing_flights} departing flights found")

        flights_objects = []

        # departing flights
        departing_flights = self.make_flight_objects_from_driver(url=self.driver.current_url)

        # add flight_combination to departing flights
        for i, flight in enumerate(departing_flights):
            flight.flight_combination = i + 1
        flights_objects.append(departing_flights)

        for i in tqdm(range(n_departing_flights)):
            # flight combination: number of the flight combination (1, 2, 3, ...) that are proposed on the page
            flight_combination = i + 1

            self.driver.get(url)

            # get list of departing flights (again, to avioid StaleElementReferenceException)
            loop_ul_sections_containig_flights = self._get_flight_lists()
            loop_departing_flight_elements_li = []
            for loop_section in loop_ul_sections_containig_flights:
                loop_flights_li_within_section = loop_section.find_elements(By.TAG_NAME, "li")
                loop_departing_flight_elements_li.extend(loop_flights_li_within_section)

            element_to_click = self._get_jsaction_element(loop_departing_flight_elements_li[i])
            element_to_click.click()

            # wait for the page to load
            try:
                # eturning flights instead of returning flights to handle both cases
                self._wait_for_text_on_page("eturning flights")
            except TimeoutException as e:
                self.driver.save_screenshot(f"TimeoutException_{i}.png")
                print("TimeoutException:", e)
                continue

            # returning flights
            returning_flights = self.make_flight_objects_from_driver(
                flight_combination=flight_combination, url=self.driver.current_url, departing_flight_id=departing_flights[i]._id
            )
            flights_objects.append(returning_flights)

        # unnest flights_data list
        flights_objects = [flight for sublist in flights_objects for flight in sublist]

        # save flight objects to class
        self.flights = flights_objects

        # close the driver
        self.driver.quit()
