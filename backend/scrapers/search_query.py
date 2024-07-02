import logging
import os
from datetime import date
from itertools import product
from typing import List, Optional

from backend.objects.airport import Airport
from backend.utils import utils
from backend.utils.utils import setup_logging

setup_logging()
logger = logging.getLogger(os.path.basename(__file__))


class SingleItemSearchQuery:
    def __init__(self, airport_dep: Airport, airport_arr: Airport, departure_date: date, return_date: Optional[date] = None):
        assert isinstance(departure_date, date), f"Departure date must be a date object, not {type(departure_date)}"
        if return_date:
            assert isinstance(return_date, date), f"Return date must be a date object, not {type(return_date)}"

        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.departure_date = departure_date
        self.return_date = return_date
        self.combinations = [self]

        logger.debug(self.__repr__())

    def __repr__(self) -> str:
        return f"SingleItemSearchQuery({self.airport_dep}, {self.airport_arr}, {self.departure_date}, {self.return_date})"


class SearchQuery:
    def __init__(
        self, airports_dep: List[Airport], airports_arr: List[Airport], departure_dates: List[date], return_dates: Optional[List[date]] = None
    ):
        # TODO: maybe add direct_only parameter here?
        """
        Initialize the SearchQuery object.

        :param airport_dep: List of departure airports (Airport objects)
        :param airport_arr: List of arrival airports (Airport objects)
        :param departure_date: List of departure dates (date objects)
        :param return_date: Optional list of return dates (date objects)
        """
        self._check_dates_validity(departure_dates, return_dates)
        self._check_airports_validity(airports_dep, airports_arr)

        self.airports_dep = airports_dep
        self.airports_arr = airports_arr
        self.departure_dates = departure_dates
        self.return_dates = return_dates

        self.combinations = self.make_single_items_combinations()

        logger.debug(self.__repr__())

    def __repr__(self) -> str:
        # handle dates as list
        departure_dates = [x.strftime("%Y-%m-%d") for x in self.departure_dates]

        if self.return_dates:
            return_dates = [x.strftime("%Y-%m-%d") for x in self.return_dates]
        else:
            return_dates = None

        airports_dep = [airport.iata for airport in self.airports_dep]
        airport_arr = [airport.iata for airport in self.airports_arr]

        # make string
        rep = f"SeachQuery({airports_dep}, {airport_arr}, {departure_dates}"

        if self.return_dates:
            rep += f", {return_dates}"

        return rep + f", [{len(self.combinations)} combinations])"

    def _check_dates_validity(self, departure_dates: List[date], return_dates: Optional[List[date]]):
        """Check the validity of the input dates."""

        # check if the lists are empty
        if not departure_dates:
            raise ValueError("Departure date list must not be empty.")
        if return_dates is not None and not return_dates:
            raise ValueError("Return date list must be either None or not empty.")

        # check if the departing dates are in the future
        for dep_date in departure_dates:
            if not utils.is_date_in_future(dep_date):
                raise ValueError(f"Departure date {dep_date} must be in the future (or today).")

        if return_dates:
            # check if the return dates are in the future
            for ret_date in return_dates:
                if not utils.is_date_in_future(ret_date):
                    raise ValueError(f"Return date {ret_date} must be in the future (or today).")

            # check if the return dates are after the departure dates
            for dep_date, ret_date in product(departure_dates, return_dates):
                if ret_date <= dep_date:
                    raise ValueError(f"Return date {ret_date} must be after departure date {dep_date}.")

    def _check_airports_validity(self, airports_dep: List[Airport], airports_arr: List[Airport]):
        """Check the validity of the input airports."""

        # check if the lists are empty
        if not airports_dep:
            raise ValueError("Departure airport list must not be empty.")
        if not airports_arr:
            raise ValueError("Arrival airport list must not be empty.")

        # check if the same airport is in both lists
        if any(airport in airports_arr for airport in airports_dep):
            raise ValueError("Departure and arrival airports must be different.")

    def make_single_items_combinations(self):
        if self.return_dates:
            return_dates = self.return_dates
        else:
            return_dates = [None]
        combinations = list(product(self.airports_dep, self.airports_arr, self.departure_dates, return_dates))
        unique_combinations = set(combinations)

        return [
            SingleItemSearchQuery(airport_dep, airport_arr, departure_date, return_date)
            for airport_dep, airport_arr, departure_date, return_date in unique_combinations
        ]
