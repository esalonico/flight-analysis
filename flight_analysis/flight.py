"""
Only direct, one-way flights on a specific day.
Example: FCO to MUC (direct) on 2024-02-25 (IT6671).
"""
import uuid
from datetime import datetime

from flight_analysis.search_query import SearchQuery

from . import utils


class Flight:
    def __init__(self, search_query: SearchQuery, flight_info: dict = dict()) -> None:
        # attributes to identify flight (input/immediately computable)
        self.search_query = search_query
        self.datetime_access = datetime.now()
        self.days_advance = utils.calculate_delta_days(self.datetime_access, self.search_query.desired_date)
        
        # attributes to scrape
        self.flight_number = flight_info.get("flight_number", None)
        self.datetime_dep = flight_info.get("datetime_dep", None)
        self.datetime_arr = flight_info.get("datetime_arr", None)
        self.price = flight_info.get("price", None)
        self.airline = flight_info.get("airline", None)
        self.duration = flight_info.get("duration", None)
        
        self._id = self._generate_id()

    def __repr__(self) -> str:
        rep = f"Flight({str(self._id)[:8]}"
        rep += f", {self.search_query.airport_dep}, {self.search_query.airport_arr}"
        rep += f", {self.search_query.desired_date}"
        rep += f", {self.datetime_dep.strftime('%H:%M')}"
        rep += f", {self.price}€, {self.airline}, {self.days_advance}d)"
        
        return rep

    def _generate_id(self) -> str:
        """
        Generates a unique ID for the flight.
        Returns: Unique ID as string.
        """
        return uuid.uuid4()