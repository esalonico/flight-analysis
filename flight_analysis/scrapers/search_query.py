from datetime import date

from flight_analysis.objects.airport import Airport
from flight_analysis.utils import utils


class SearchQuery:
    def __init__(self, airport_dep: Airport, airport_arr: Airport, departure_date: date):
        assert utils.is_date_in_future(departure_date), "Desired date must be in the future (or today)."

        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.departure_date = departure_date

    def __repr__(self) -> str:
        return f"SearchQuery({self.airport_dep}, {self.airport_arr}, {self.departure_date})"
