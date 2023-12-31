from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
import re

from flight_analysis.flight import Flight


class Scrape:
    def __init__(self, flight: Flight) -> None:
        self.flight = flight
        
        self.driver = self._create_driver()
        self.url = self._build_url(flight)
        
        
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
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept all')]"))).click()
        
    
    def _get_raw_flight_results(self, driver: webdriver.Chrome, url: str) -> list:
        """
        Reaches the flight results page. Also handles auto acceptance of Google's Terms & Conditions page.
        """
        timeout = 15
        driver.get(url)
        
        # deal with Google's term and conditions page
        self._skip_google_terms_page(driver, timeout)
        
        # take screenshot of page
        driver.save_screenshot(f"screenshot.png")
        
        return driver.find_element(by=By.XPATH, value='//body[@id = "yDmH0d"]').text.split("\n")
    
    def _filter_raw_results(self, results_raw: list) -> list:
        """
        Filters the raw results to only include the flights-related elements of the list.
        This is done by identifying the start and end index of the flights-related elements.
        Returns: Cleaned list of flights-related elements.
        """
        start_regex = re.compile("Sort by:")
        end_regex = re.compile("Language")

        start_idx = [i for i, el in enumerate(results_raw) if start_regex.match(el)][0] + 1
        end_idx = [i for i, el in enumerate(results_raw) if end_regex.match(el)][0]
        
        return results_raw[start_idx:end_idx]
            
            




class DirectOneWayScraper(Scrape):
    def __init__(self, flight: Flight) -> None:
        super().__init__(flight)

    def _build_url(self, flight: Flight) -> str:
        """
        Builds the URL to scrape.
        Returns: URL as string.
        """
        url = "https://www.google.com/travel/flights"
        url += f"?q=Flights%20to%20{flight.airport_arr.iata}"
        url += f"%20from%20{flight.airport_dep.iata}"
        url += f"%20on%20{flight.desired_date}%20oneway%20direct&curr=EUR&gl=IT"

        return url

    def scrape(self) -> None:
        results_raw = super()._get_raw_flight_results(self.driver, self.url)
        results_raw_filtered = super()._filter_raw_results(results_raw)
        print(results_raw_filtered)