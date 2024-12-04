from datetime import date
from typing import List

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver, WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import src.flights.scrapers.extractors as extractors
from src.flights.models.models import Flight


def is_google_terms_and_conditions_page(page_source: str) -> bool:
    """
    Returns True if the page is Google's Terms and Conditions page.
    """
    return "Before you continue to Google" in page_source


def skip_google_terms_and_conditions_page(driver: webdriver.Chrome, timeout: int = 10):
    """
    Clicks the "I agree" button on Google's Terms and Conditions page.

    :param driver: Chrome webdriver instance.
    :param timeout: Time in seconds to wait for the button
    """
    # WebDriverWait(driver, timeout).until(lambda s: is_google_terms_and_conditions_page(s.page_source))

    # click on accept terms button
    WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept all')]"))).click()


def click_on_cheapest_tab(driver: WebDriver, timeout: int = 10):
    """
    Waits for and clicks on the element corresponding to the tab "cheapest" in Google Flights.

    :param driver: Chrome WebDriver instance.
    :param timeout: Maximum time to wait for the element to be clickable, in seconds.
    """
    # wait until the element is clickable
    element = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((By.ID, "M7sBEb")))

    # click the element
    element.click()


def wait_for_cheapest_prices_to_load(driver: WebDriver, timeout: int = 10):
    """
    Waits for the small cheapest prices loading spinner to become visible and then diasppear, which means all prices have loaded.

    :param driver: Chrome WebDriver instance.
    :param timeout: Maximum time to wait for the spinner to appear and disappear, in seconds.
    """
    WebDriverWait(driver, timeout).until(EC.visibility_of_element_located((By.CLASS_NAME, "m2B2uc")))
    WebDriverWait(driver, timeout).until(EC.invisibility_of_element((By.CLASS_NAME, "m2B2uc")))

    # also wait until the text "Top options" or "Top flights" or "All flights" is visible
    WebDriverWait(driver, timeout).until(
        lambda d: any(
            text in d.find_element(By.XPATH, "//div[contains(., 'Top options') or contains(., 'Top flights') or contains(., 'All flights')]").text
            for text in ["Top options", "Top flights", "All flights"]
        )
    )


def get_html_sections_containing_flight_data(driver: WebDriver, timeout: int = 15) -> List[WebElement]:
    """
    Waits for and returns all HTML sections that contains flight data (i.e. Top flights, Other flights...).
    (Waits until all <ul> elements with the specified class are visible and returns them as a list of WebElements.)

    # TODO: handle no results found

    :param driver: Chrome webdriver instance.
    :param timeout: Maximum time to wait for the elements to be visible, in seconds.
    :return: A list of WebElements representing all <ul> elements with the specified class.
    :raises TimeoutException: If no elements become visible within the timeout.
    """
    sections = WebDriverWait(driver, timeout).until(EC.visibility_of_all_elements_located((By.CLASS_NAME, "Rk10dc")))

    if not sections:
        raise NotImplementedError("Implement case: no results found")

    return sections


def extract_flight_data_from_li(element: WebElement, dep_date: date) -> Flight:
    """
    Extract flight data from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :param dep_date: Departure date of the flight.
    :return: Flight object containing the extracted data.
    """
    data = {}
    data["origin"], data["destination"] = extractors.extract_origin_and_destination(element)
    data["dep_datetime"], data["arr_datetime"] = extractors.extract_departure_and_arrival_datetimes(element, dep_date)
    data["airline"] = extractors.extract_airline_name(element)
    data["flight_time"] = extractors.extract_flight_time(element)
    data["n_stops"] = extractors.extract_number_of_stops(element)
    data["price"] = extractors.extract_price(element)
    data["stops"] = extractors.extract_stops(element)
    data["only_hand_luggage"] = extractors.extract_only_hand_luggage(element)
    data["airline_logo_url"] = extractors.extract_airline_logo_url(element)

    return Flight(**data)
