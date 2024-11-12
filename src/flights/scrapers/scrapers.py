from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from src.flights.models.models import SearchItem


class BaseScraper:
    def __init__(self, search_item: SearchItem) -> None:
        self.search_item = search_item

        self.driver = self._create_driver()
        self.url = self._build_url()

    def _create_driver(self) -> webdriver.Chrome:
        """
        Creates a Chrome webdriver instance.

        :return: Chrome webdriver instance.
        """
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")

        return webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )

    def _build_url(self) -> str:
        raise NotImplementedError("Method must be implemented in subclass.")


class OneWayScraper(BaseScraper):
    def _build_url(self) -> str:
        """
        Build the URL for the flight search.

        :return: URL string for the flight search
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{self.search_item.origin.iata}%20Airport"
        url += f"%20from%20{self.search_item.destination.iata}"

        if self.search_item.direct_only:
            return f"{url}%20on%20{self.search_item.departure_date}%20oneway%20direct&curr=EUR&gl=IT"

        return f"{url}%20on%20{self.search_item.departure_date}%20oneway&curr=EUR&gl=IT"
