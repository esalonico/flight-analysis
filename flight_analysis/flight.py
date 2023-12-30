from datetime import date, datetime

from . import utils
from .airport import Airport


class Flight:
    def __init__(self, airport_dep: Airport, airport_arr: Airport, desired_date: date) -> None:
        # attributes to identify flight (input/immediately computable)
        self.airport_dep = airport_dep
        self.airport_arr = airport_arr
        self.desired_date = utils.check_date_in_future(desired_date)
        self.datetime_access = datetime.now()
        self.days_advance = utils.calculate_delta_days(self.datetime_access, self.desired_date)
        self.scraped = False
        
        # attributes to scrape
        self.flight_number = None
        self.datetime_dep = None
        self.datetime_arr = None
        self.price = None
        self.airline = None
        self.duration = None

    def __repr__(self) -> str:
        return f"Flight({self.flight_number}, {self.airport_dep}, {self.airport_arr}, {self.desired_date}, {self.days_advance}, {self.scraped})"

