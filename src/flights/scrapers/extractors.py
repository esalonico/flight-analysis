"""Extract flight data from WebElements containing flight information."""

import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebElement

from src.flights.models.models import Airport


def extract_origin_and_destination(element: WebElement) -> Tuple[Airport, Airport]:
    """
    Extract the origin and destination airports from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :return: Tuple of two Airport objects representing the origin and destination airports.
    """
    airport_codes = element.find_element(By.CSS_SELECTOR, ".PTuQse.sSHqwe.tPgKwe.ogfYpf").text

    origin, destination = airport_codes.split("–")

    return Airport(iata=origin), Airport(iata=destination)


def extract_airline_logo_url(element: WebElement) -> str:
    """
    Extract the URL of the airline logo from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :return: URL of the airline logo.
    """
    data = element.find_element(By.CLASS_NAME, "EbY4Pc").get_attribute("style")
    match = re.search(r"url\((.*?)\)", data)

    return match.group(1)


def extract_departure_and_arrival_datetimes(element: WebElement, dep_date: date) -> Tuple[datetime, datetime]:
    """
    Extract the departure and arrival datetimes from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :param dep_date: Departure date of the flight.
    :return: Tuple of two datetime objects representing the departure and arrival datetimes.
    """
    data = element.find_element(By.CLASS_NAME, "mv1WYe").text

    # handle +- X days
    if "+" in data:
        delta_days = int(data.split("+")[-1])
        arr_date = dep_date + timedelta(days=delta_days)
        data = data.split("+")[0]
    elif "-" in data:
        delta_days = int(data.split("-")[-1])
        arr_date = dep_date - timedelta(days=delta_days)
        data = data.split("-")[0]
    else:
        arr_date = dep_date

    dep_time, arr_time = data.replace("\n", "").replace("\u202f", "").split(" – ")

    dep_datetime = datetime.combine(dep_date, datetime.strptime(dep_time, "%I:%M%p").time())
    arr_datetime = datetime.combine(arr_date, datetime.strptime(arr_time, "%I:%M%p").time())

    return dep_datetime, arr_datetime


def extract_airlines_names(element: WebElement) -> List[str]:
    """
    Extract the name of the airline from a <li> WebElement containing flight information.

    # TODO: Fix "Operated by" or separator in airlines names.

    :param element: <li> WebElement containing flight data.
    :return: Name of the airline.
    """
    airlines = element.find_element(By.CSS_SELECTOR, ".sSHqwe.tPgKwe.ogfYpf").text
    airlines = airlines.replace("Self transfer", "").strip()
    return airlines.split(", ")


def extract_flight_time(element: WebElement) -> int:
    """
    Extract the flight time from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :return: Flight time (in minutes) as an int.
    """
    flight_time_str = element.find_element(By.CSS_SELECTOR, ".gvkrdb.AdWm1c.tPgKwe.ogfYpf").text
    return get_duration_in_minutes_from_string(flight_time_str)


def extract_layover_information(element: WebElement) -> Tuple[int, Optional[List[str]], Optional[int]]:
    """
    Extracts layover information from a flight element.

    :param element: <li> WebElement containing flight data.
    :return:
        - Number of stops (int)
        - Layover location(s) Optional(List[str])
        - Layover time (in minutes) Optional(int)
    """
    n_stops, layover_location, layover_time = None, None, None

    layover_section = element.find_element(By.CLASS_NAME, "BbR8Ec")
    layover_text_raw = layover_section.text
    layover_text_list = layover_text_raw.split("\n")

    # 1. number of stops
    layover_stops_text = layover_text_list[0]
    if layover_stops_text == "Nonstop":
        return 0, None, None
    else:
        n_stops = int(layover_stops_text.split(" ")[0])

    # 2. layover location
    layover_time_and_location_text = layover_text_list[1]
    location_match = re.search(r"([A-Z]{3}(?:, [A-Z]{3})*)$", layover_time_and_location_text)
    layover_location = location_match.group().split(", ") if location_match else None

    # 3. layover time
    if n_stops >= 2:  # if there are 2 or more stops, layover time is not shown
        return n_stops, layover_location, None

    if layover_location:
        layover_location_str = ", ".join(layover_location)
        layover_time_and_location_text = layover_time_and_location_text.replace(layover_location_str, "").strip()

    layover_time = get_duration_in_minutes_from_string(layover_time_and_location_text)

    return n_stops, layover_location, layover_time


def extract_only_hand_luggage(element: WebElement) -> bool:
    """
    Extract whether the flight allows only hand luggage from a <li> WebElement containing flight information.

    :param element: <li> WebElement containing flight data.
    :return: True if only hand luggage is allowed, False if not.
    """
    try:
        return element.find_element(By.CSS_SELECTOR, ".vmWDCc.NMm5M") is not None
    except NoSuchElementException:
        return False


def extract_price(element: WebElement) -> Optional[int]:
    """
    Extract the price in EUR from a <li> WebElement containing flight information.
    If the price is unavailable, return None.

    :param element: <li> WebElement containing flight data.
    :return: Price (in €) as an int or None if price is unavailable.
    """
    price = element.find_element(By.CSS_SELECTOR, ".YMlIz.FpEdX").text

    if "Price unavailable" in price:
        return None

    return int(price.replace("€", "").replace(",", ""))


def get_duration_in_minutes_from_string(s: str) -> int:
    """
    Returns the duration in minutes from a string.

    :param s: Duration string in the format "X hr Y min" or "X min".
    :return: Duration in minutes.

    Examples:
        3 hr 20 min --> 60*3 + 20 = 200
        20 min --> 20
        5 hr 55 min --> 60*5 + 55 = 355
    """
    assert bool(re.search("hr|min", str(s))), "Invalid duration string format"

    match = re.match(r"(?:(\d+) hr)? ?(?:(\d+) min)?", s)
    h, m = match.groups(default="0")
    return int(h) * 60 + int(m)
