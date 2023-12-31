from datetime import date, datetime

from . import utils
from .airport import Airport


class SearchQuery():
    def __init__(self, airport_dep: Airport, airport_arr: Airport, desired_date: date) -> None:
        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.desired_date = utils.check_date_in_future(desired_date)
        
    def __repr__(self) -> str:
        return f"SearchQuery({self.airport_dep}, {self.airport_arr}, {self.desired_date})"