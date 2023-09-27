# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import re
import os
import chromedriver_autoinstaller

from src.flight_analysis.flight import Flight

# logging
logger_name = os.path.basename(__file__)
logger = logging.getLogger(logger_name)


class Scrape:
    def __init__(self, orig, dest, date_leave, date_return=None):
        self._orig = orig
        self._dest = dest
        self._date_leave = date_leave
        self._date_return = date_return
        self._round_trip = True if date_return is not None else False
        self._data = None
        self._url = None

    @property
    def data(self):
        return self._data

    def __repr__(self):
        if self._date_return is None:
            return "{dl}: {org} --> {dest}".format(
                dl=self._date_leave, org=self._orig, dest=self._dest
            )
        else:
            return "{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
                dl=self._date_leave,
                dr=self._date_return,
                org=self._orig,
                dest=self._dest,
            )

    def run_scrape(self):
        self._data = self._scrape_data()

    # TODO: reactivate
    # def _create_driver(self):
    #     """
    #     Creates a Chrome webdriver instance.
    #     """
    #     options = Options()
    #     options.add_argument("--no-sandbox")
    #     options.add_argument("--headless")
    #     options.add_argument(
    #         "--window-size=1920,1080"
    #     )  # otherwise data such as layover location and emissions is not displayed

    #     driver = webdriver.Chrome(
    #         service=Service(ChromeDriverManager().install()), options=options
    #     )

    #     return driver

    # TODO: delete
    def _create_driver(self):
        """
        Creates a Chrome webdriver instance.
        """
        service = Service()
        options = Options()
        options.add_argument("--no-sandbox")
        options.add_argument("--headless")
        options.add_argument(
            "--window-size=1920,1080"
        )  # otherwise data such as layover location and emissions is not displayed

        driver = webdriver.Chrome(service=service, options=options)

        return driver

    def _scrape_data(self):
        """
        Scrapes the Google Flights page and returns a DataFrame of the results.
        """
        driver = self._create_driver()
        self._url = self._make_url()
        flight_results = self._get_results(driver)
        driver.quit()

        return flight_results

    def _make_url(self):
        """
        From the class parameters, generates a dynamic Google Flight URL to scrape, taking into account if the
        trip is one way or roundtrip.
        """
        if self._round_trip:
            return "https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20from%20{date_leave}%20to%20{date_return}&curr=EUR&gl=IT".format(
                dest=self._dest,
                org=self._orig,
                date_leave=self._date_leave,
                date_return=self._date_return,
            )
        else:
            return "https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20on%20{date_leave}%20oneway&curr=EUR&gl=IT".format(
                dest=self._dest, org=self._orig, date_leave=self._date_leave
            )

    def _get_results(self, driver):
        """
        Returns the scraped flight results as a DataFrame.
        """
        results = None
        try:
            results = Scrape._make_url_request(self._url, driver)
            if not results:
                return None

        except TimeoutException:
            logger.error(
                "Scrape timeout reached. It could mean that no flights exist for the combination of airports and dates."
            )
            return None

        flights = self._clean_results(results)
        return Flight.make_dataframe(flights)

    def _clean_results(self, result):
        """
        Cleans and organizes the raw text strings scraped from the Google Flights results page.
        """
        res2 = [x.encode("ascii", "ignore").decode().strip() for x in result]
        price_trend_dirty = [x for x in res2 if x.startswith("Prices are currently")]
        price_trend = Scrape.extract_price_trend(price_trend_dirty)

        footer_reached = False
        start = res2.index("Sort by:") + 1

        try:
            mid_start = res2.index("Price insights")
        except ValueError:
            try:
                mid_start = res2.index("Other flights")
            except ValueError:
                mid_start = None
            
        mid_end = -1

        try:
            mid_end = res2.index("Other departing flights") + 1
        except:
            try:
                mid_end = res2.index("Other flights") + 1
            except ValueError:
                # basically, identify the footer
                last_index = [i for i, s in enumerate(res2) if 'Language' in s]
                if len(last_index) > 0:
                    mid_end = last_index[-1]
                    footer_reached = True

        if footer_reached:
            res3 = res2[start:mid_end]
        else:
            end = [i for i, x in enumerate(res2) if "more flight" in x][0]
            if mid_start:
                res3 = res2[start:mid_start] + res2[mid_end:end]
            else:
                res3 = res2[start:end]
                

        # Enumerate over the list 'res3'
        matches = []
        for index, element in enumerate(res3):
            # Check if element is not an empty string
            if len(element) == 0:
                continue

            # Check if the element ends with 'AM' or 'PM' (or AM+, PM+)
            is_time_format = bool(
                re.search("\d{1,2}\:\d{2}(?:AM|PM)\+{0,1}\d{0,1}", element)
            )

            # If the element doesn't end with '+' and is in time format, then add it to the matches list
            if element[-2] != "+" and is_time_format:
                matches.append(index)
            
        # special case: scrape has only one flight
        if len(matches) == 2:
            matches.append(len(res3))
            
        # handles the identification of whole flights, instead of splitting every
        # time a time is found
        # TODO: document better
        matches_ok = [matches[0]]

        for i in range(1, len(matches)):
            if matches[i] - matches[i - 1] < 4:
                continue
            matches_ok.append(matches[i])

        flights = []
        for i in range(len(matches_ok) - 1):
            flight_args = res3[matches_ok[i] : matches_ok[i + 1]]

            if len(flight_args) > 5:
                f = Flight(
                    self._date_leave,  # date_leave
                    self._round_trip,  # round_trip
                    self._orig,
                    self._dest,
                    price_trend,
                    flight_args,
                )
                flights.append(f)

        return flights

    @staticmethod
    def extract_price_trend(s):
        """
        From a dirty string, return a tuple in format (price_trend, trend value) for a given flight.
        For example:
        (typical, None): Prices for that dates/airports are currently average
        (low, 100): Prices are lower than usual by 100€
        (high, None): Prices are higher than usual
        """
        if not s:
            return (None, None)

        s = s[0]
        if s == "Prices are currently typical":
            return ("typical", None)

        elif s == "Prices are currently high":
            return ("high", None)

        elif "cheaper" in s:
            how_cheap = int([x for x in s.split(" ") if x.isdigit()][0])
            return ("low", how_cheap)

        else:
            return (None, None)

    @staticmethod
    def _identify_google_terms_page(page_source: str):
        """
        Returns True if the page html represent Google's Terms and Coditions page.
        """
        if "Before you continue to Google" in page_source:
            return True
        return False

    @staticmethod
    def _make_url_request(url, driver):
        """
        Get raw results from Google Flights page.
        Also handles auto acceptance of Google's Terms & Conditions page.
        """
        timeout = 15
        driver.get(url)

        # detect Google's Terms & Conditions page (not always there, only in EU)
        if Scrape._identify_google_terms_page(driver.page_source):
            WebDriverWait(driver, timeout).until(
                lambda s: Scrape._identify_google_terms_page(s.page_source)
            )

            # click on accept terms button
            WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(., 'Accept all')]")
                )
            ).click()

        # wait for flight data to load and initial XPATH cleaning
        WebDriverWait(driver, timeout).until(
            lambda d: len(Scrape._get_flight_elements(d)) > 50
        )
        results = Scrape._get_flight_elements(driver)
        
        # special case: no flights found ("Sort by:" string is always displayed when there are flights)
        if "Sort by:" not in results:
            return None

        return results

    @staticmethod
    def _get_flight_elements(driver):
        """
        Returns all html elements that contain/have to do with flight data.
        """
        return driver.find_element(
            by=By.XPATH, value='//body[@id = "yDmH0d"]'
        ).text.split("\n")
