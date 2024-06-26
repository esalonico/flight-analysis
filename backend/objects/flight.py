"""
Only direct, one-way flights on a specific day.
Example: FCO to MUC (direct) on 2024-02-25 (IT6671).
"""

import uuid
from datetime import datetime

from backend.scrapers.search_query import SearchQuery
from backend.utils import utils


class Flight:
    def __init__(self, search_query: SearchQuery, flight_info: dict = dict(), datetime_access: datetime = datetime.now()):
        self._id = uuid.uuid4()
        self._search_query = search_query

        # attributes to identify flight (input/immediately computable)
        self.datetime_access = datetime_access
        self.days_advance = utils.calculate_delta_days(self.datetime_access, self._search_query.departure_date)

        # attributes to scrape
        self.airport_dep = flight_info.get("airport_dep", None)
        self.airport_arr = flight_info.get("airport_arr", None)

        self.datetime_dep = flight_info.get("datetime_dep", None)
        self.datetime_arr = flight_info.get("datetime_arr", None)

        self.price = flight_info.get("price", None)

        self.n_stops = flight_info.get("n_stops", None)
        self.stops = flight_info.get("stops", None)
        self.stop_layover_time = flight_info.get("stop_layover_time", None)

        self.airline = flight_info.get("airline", None)
        self.duration = flight_info.get("duration", None)
        self.flight_combination = flight_info.get("flight_combination", None)
        self.url = flight_info.get("url", None)
        self.departing_flight_id = flight_info.get("departing_flight_id", None)

    def __repr__(self) -> str:
        rep = f"Flight({str(self._id)[:8]}"
        rep += f", {self.airport_dep}, {self.airport_arr}"
        rep += f", {self._search_query.departure_date}"
        rep += f", {self.datetime_dep.strftime('%H:%M')}"
        rep += f", {self.price}€, {self.airline}, {self.days_advance}d)"

        return rep

    def to_dict(self) -> dict:
        """
        Returns a dictionary with all public attributes of the flight.
        """
        # don't return private attributes
        return {"_id": self._id} | {key: value for key, value in self.__dict__.items() if not key.startswith("_")}
