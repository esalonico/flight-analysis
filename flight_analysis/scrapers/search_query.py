from datetime import date

from flight_analysis.objects.airport import Airport
from flight_analysis.utils import utils


class SearchQuery:
    def __init__(self, airport_dep: Airport, airport_arr: Airport, departure_date: date, return_date: date = None):
        assert utils.is_date_in_future(departure_date), "Desired date must be in the future (or today)."

        if return_date:
            assert utils.is_date_in_future(return_date), "Return date must be in the future (or today)."
            assert return_date > departure_date, "Return date must be after departure date."

        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.departure_date = departure_date
        self.return_date = return_date

    def __repr__(self) -> str:
        rep = f"SearchQuery({self.airport_dep}, {self.airport_arr}, {self.departure_date}"
        if self.return_date:
            rep += f", {self.return_date})"

        return rep
