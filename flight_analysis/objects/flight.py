"""
Only direct, one-way flights on a specific day.
Example: FCO to MUC (direct) on 2024-02-25 (IT6671).
"""

import uuid
from datetime import datetime

from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils import utils


class Flight:
    def __init__(self, search_query: SearchQuery, flight_info: dict = dict()):
        self._id = uuid.uuid4()
        self._search_query = search_query

        # attributes to identify flight (input/immediately computable)
        self.datetime_access = datetime.now()
        self.days_advance = utils.calculate_delta_days(self.datetime_access, self._search_query.departure_date)

        # attributes to scrape
        self.flight_number = flight_info.get("flight_number", None)
        self.datetime_dep = flight_info.get("datetime_dep", None)
        self.datetime_arr = flight_info.get("datetime_arr", None)
        self.price = flight_info.get("price", None)
        self.airline = flight_info.get("airline", None)
        self.duration = flight_info.get("duration", None)

    def __repr__(self) -> str:
        rep = f"Flight({str(self._id)[:8]}"
        rep += f", {self._search_query.airport_dep}, {self.search_query.airport_arr}"
        rep += f", {self._search_query.departure_date}"
        rep += f", {self.datetime_dep.strftime('%H:%M')}"
        rep += f", {self.price}€, {self.airline}, {self.days_advance}d)"

        return rep

    def to_dict(self) -> dict:
        """
        Returns a dictionary with all public attributes of the flight.
        """
        # don't return private attributes
        return {key: value for key, value in self.__dict__.items() if not key.startswith("_")}
