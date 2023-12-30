from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

from flight_analysis.flight import Flight


class Scrape:
    def __init__(self) -> None:
        self.url = None
        
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



class DirectOneWayScraper(Scrape):
    def __init__(self, flight: Flight) -> None:
        self.flight = flight
        
        self.driver = self._create_driver()
        self.url = self._build_url(flight)
        
        self.driver.quit()  # TODO: handle this

    def _build_url(self, flight: Flight) -> str:
        """
        Builds the URL to scrape.
        Returns: URL as string.
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{flight.airport_arr.iata}"
        url += f"%20from%20{flight.airport_dep.iata}"
        url += f"%20on%20{flight.desired_date}%20oneway&curr=EUR&gl=IT"

        return url

    def scrape(self) -> None:
        raise NotImplementedError("TODO")