from datetime import date
from typing import List, Optional

from backend.objects.airport import Airport
from backend.utils import utils


class BaseSearchQuery:
    def __init__(self):
        pass

    def __repr__(self) -> str:
        # handle dates as list
        if isinstance(self.departure_date, list):
            departure_date = [x.strftime("%Y-%m-%d") for x in self.departure_date]
        else:
            departure_date = self.departure_date.strftime("%Y-%m-%d")

        if self.return_date and isinstance(self.return_date, list):
            return_date = [x.strftime("%Y-%m-%d") for x in self.return_date]
        elif self.return_date:
            return_date = self.return_date.strftime("%Y-%m-%d")
        else:
            return_date = None

        # make string
        rep = f"{self.__class__.__name__}({self.airport_dep}, {self.airport_arr}, {departure_date}"

        if self.return_date:
            rep += f", {return_date}"

        return rep + ")"


class SimpleSearchQuery(BaseSearchQuery):
    """
    Simple search query.

    – 1 departure airport (Airport)
    – 1 arrival airport (Airport)
    – 1 departure date (date)
    – 1 return date (optional) (date)
    """

    def __init__(self, airport_dep: Airport, airport_arr: Airport, departure_date: date, return_date: date = None):
        assert utils.is_date_in_future(departure_date), "Desired date must be in the future (or today)."

        if return_date:
            assert utils.is_date_in_future(return_date), "Return date must be in the future (or today)."
            assert return_date > departure_date, "Return date must be after departure date."

        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.departure_date = departure_date
        self.return_date = return_date


class MultiDateSearchQuery(BaseSearchQuery):
    """
    Multi-date search query.

    – 1 departure airport (Airport)
    – 1 arrival airport (Airport)
    – N departure date (List[date])
    – 1-N return date (List[date]) (optional)
    """

    def __init__(self, airport_dep: Airport, airport_arr: Airport, departure_date: List[date], return_date: Optional[List[date]] = None):
        # assert departure_date and return_date are lists
        assert isinstance(departure_date, list), "Departure date must be a list."
        assert return_date is None or isinstance(return_date, list), "Return date must be a list or None."

        # assert departure_date and return_date are not empty
        assert departure_date, "Departure date must not be empty."
        assert return_date is None or return_date, "Return date must not be empty."

        # assert dates are in the future
        for dep_date in departure_date:
            assert utils.is_date_in_future(dep_date), "Desired date must be in the future (or today)."
        if return_date:
            for ret_date in return_date:
                assert utils.is_date_in_future(ret_date), "Return date must be in the future (or today)."

        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.departure_date = departure_date
        self.return_date = return_date
