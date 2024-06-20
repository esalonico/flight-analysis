from backend.scrapers.base_scraper import BaseScraper
from backend.scrapers.search_query import SimpleSearchQuery

 
class OneWayScraper(BaseScraper):
    def __init__(self, search_query: SimpleSearchQuery, direct_only: bool):
        """
        Initialize the OneWayScraper.

        :param search_query: SimpleSearchQuery object with the search parameters
        :param direct_only: Whether to search for direct flights only (True) or not (False)
        """
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

        if self.direct_only:
            url += f"%20on%20{self.search_query.departure_date}%20oneway%20direct&curr=EUR&gl=IT"
        else:
            url += f"%20on%20{self.search_query.departure_date}%20oneway&curr=EUR&gl=IT"

        return url

    def scrape(self):
        """
        Scrapes the flight results page.
        Updates the flights attribute of the class.
        """
        self.driver.get(self.url)

        # deal with Google's term and conditions page
        self._skip_google_terms_page(self.driver, 15)

        flight_objects = super().make_flight_objects_from_driver(url=self.url)

        # save flight objects to class
        self.flights = flight_objects

        # close the driver
        self.driver.quit()
