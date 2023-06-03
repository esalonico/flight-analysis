# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from datetime import date, datetime, timedelta
import os
import numpy as np
import pandas as pd
from tqdm import tqdm

import sys
sys.path.append('src/google_flight_analysis')
from flight import Flight

__all__ = ['Scrape', '_Scrape']

class _Scrape:

    def __init__(self):
        self._origin = None
        self._dest = None
        self._date_leave = None
        self._date_return = None
        self._round_trip = None
        self._data = None

        self.results_dirty = None
        self.results_clean = None
        self.url = None


    def __call__(self, *args, export=False):
        self._set_properties(*args)
        self._data = self._scrape_data()
        obj = self.clone(*args)
        
        obj.data = self._data
        obj.results_clean = self.results_clean
        obj.results_dirty = self.results_dirty
        obj.url = self.url
        
        if export:
            Flight.export_to_csv(obj.data,obj._origin, obj._dest, obj._date_leave, obj._date_return)
        
        return obj


    def __str__(self):
        if self._date_return is None:
            return "{dl}: {org} --> {dest}".format(
            dl = self._date_leave,
            org = self._origin,
            dest = self._dest
        )
        else:
            return "{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
                dl = self._date_leave,
                dr = self._date_return,
                org = self._origin,
                dest = self._dest
            )

    def __repr__(self):
        if self._date_return is None:
            return "{n} RESULTS FOR:\n{dl}: {org} --> {dest}".format(
                n = self._data.shape[0],
                dl = self._date_leave,
                org = self._origin,
                dest = self._dest
            )
        else:
            return "{n} RESULTS FOR:\n{dl}: {org} --> {dest}\n{dr}: {dest} --> {org}".format(
                n = self._data.shape[0],
                dl = self._date_leave,
                dr = self._date_return,
                org = self._origin,
                dest = self._dest
            )

    def clone(self, *args):
        obj = _Scrape()
        obj._set_properties(*args)
        return obj


    def _set_properties(self, *args):
        """
        Set properties from args when the class is called.
        """
        if len(args) >= 4:
            prop = args
        else:
            prop = (args + (None,))

        (self._origin, self._dest, self._date_leave, self._date_return) = prop
        
        # compute if the trip is a round trip
        self._round_trip = (False if self._date_return is None else True)
  
        
    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, x : str) -> None:
        self._origin = x

    @property
    def dest(self):
        return self._dest

    @dest.setter
    def dest(self, x : str) -> None:
        self._dest = x

    @property
    def date_leave(self):
        return self._date_leave

    @date_leave.setter
    def date_leave(self, x : str) -> None:
        self._date_leave = x

    @property
    def date_return(self):
        return self._date_return

    @date_return.setter
    def date_return(self, x : str) -> None:
        self._date_return = x

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        self._data = x

    '''
        Scrape the object
    '''
    def _scrape_data(self):
        driver = webdriver.Chrome()
        # driver.maximize_window()
        url = self._make_url()
        self.url = url
        flight_results = self._get_results(url, driver)
        driver.quit()
        return flight_results


    def _make_url(self):
        """
        From the class parameters, generates a dynamic Google Flight URL to scrape, taking into account if the
        trip is one way or roundtrip.
        """
        if self._round_trip:
            return 'https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20from%20{date_leave}%20to%20{date_return}'.format(
                dest = self._dest,
                org = self._origin,
                date_leave = self._date_leave,
                date_return = self._date_return)
        else:
            return 'https://www.google.com/travel/flights?q=Flights%20to%20{dest}%20from%20{org}%20on%20{date_leave}%20oneway'.format(
                dest = self._dest,
                org = self._origin,
                date_leave = self._date_leave
            )


    def _get_results(self, url, driver):
        """
        Returns the scraped flight results as a DataFrame.
        """
        results = None
        try:
            results = _Scrape._make_url_request(url, driver)
            self.results_dirty = results
        except TimeoutException:
            print(
                '''TimeoutException, try again and check your internet connection!\n
                Also possible that no flights exist for your query :('''.replace('\t','')
            )
            return -1

        flights = self._clean_results(results)
        self.results_clean = flights
        return Flight.dataframe(flights)

    def _clean_results(self, result):
        """
        Cleans and organizes the raw text strings scraped from the Google Flights results page.
        """
        res2 = [x.encode("ascii", "ignore").decode().strip() for x in result]
        
        price_trend_dirty = [x for x in res2 if x.startswith("Prices are currently")]
        price_trend = Scrape.extract_price_trend(price_trend_dirty)

        start = res2.index("Sort by:")+1
        
        try:
            mid_start = res2.index("Price insights")
        except ValueError:
            mid_start = res2.index("Other flights")
        mid_end = -1
        
        try:
            mid_end = res2.index("Other departing flights")+1
        except:
            mid_end = res2.index("Other flights")+1
        
        end  = [i for i, x in enumerate(res2) if x.endswith('more flights')][0]

        res3 = res2[start:mid_start] + res2[mid_end:end]

        matches = [i for i, x in enumerate(res3) if len(x) > 2 and ((x[-2] != '+' and (x.endswith('PM') or x.endswith('AM'))) or x[-2] == '+')][::2]


        flights = [
            Flight(
                self._date_leave,
                self._round_trip,
                price_trend,
                res3[matches[i]:matches[i+1]]) for i in range(len(matches)-1)
            ]

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

        # detect Google's Terms & Conditions page
        WebDriverWait(driver, timeout).until(lambda s: Scrape._identify_google_terms_page(s.page_source))
        
        # click on accept terms button
        WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept all')]"))).click()

        # wait for flight data to load and initial XPATH cleaning
        WebDriverWait(driver, timeout).until(lambda d: len(_Scrape._get_flight_elements(d)) > 100)
        results = _Scrape._get_flight_elements(driver)

        return results

    @staticmethod
    def _get_flight_elements(driver):
        """
        Returns all html elements that contain/have to do with flight data.
        """
        return driver.find_element(by = By.XPATH, value = '//body[@id = "yDmH0d"]').text.split('\n')

Scrape = _Scrape()