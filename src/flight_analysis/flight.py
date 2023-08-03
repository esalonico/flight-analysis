# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

from datetime import datetime, timedelta
import pandas as pd
import re
from os import path


class Flight:
    def __init__(self, dl, roundtrip, queried_orig, queried_dest, price_trend, *args):
        self._roundtrip = roundtrip
        self._origin = None
        self._queried_orig = queried_orig
        self._dest = None
        self._queried_dest = queried_dest
        self._date = dl
        self._dow = datetime.strptime(dl, "%Y-%m-%d").isoweekday()  # day of week
        self._airline = None
        self._flight_time = None
        self._num_stops = None
        self._stops = None
        self._stops_locations = None
        self._co2 = None
        self._emissions = None
        self._price = None
        self._price_trend = price_trend
        self._times = []
        self._time_leave = None
        self._time_arrive = None
        self._has_train = False
        self._trash = []

        # extract the values above from the scraped HTML page source
        self._parse_args(*args)

    def __repr__(self):
        return f"{self._origin}-{self._dest}-{self._date}"

    def _classify_arg(self, arg: str):
        """
        Classifies a string (arg) into the correct attribute for a flight,
        such as price, numer of layover stops, arrival time...
        """

        # define cases for which to return early
        arg_empty = arg is None or arg == ""
        arg_useless = arg in ["Change of airport", "round trip", "Climate friendly"]
        arg_delay = arg.startswith("Delayed")
        early_return_conditions = [arg_empty, arg_useless, arg_delay]

        # return early
        if any(early_return_conditions):
            return

        # airline: Separate tickets booked together
        if arg == "Separate tickets booked together":
            self._airline = ["multiple"]

        # arrival or departure time
        # regex: AM/PM (for example: 10:30AM, 4:11PM)
        elif bool(re.search("\d{1,2}\:\d{2}(?:AM|PM)\+{0,1}\d{0,1}", arg)) and (
            len(self._times) < 2
        ):
            delta = timedelta(days=0)
            if arg[-2] == "+":
                delta = timedelta(days=int(arg[-1]))
                arg = arg[:-2]

            date_format = "%Y-%m-%d %I:%M%p"
            self._times += [
                datetime.strptime(self._date + " " + arg, date_format) + delta
            ]

        # flight time
        # regex:  3 hr 35 min, 45 min, 5 hr
        elif bool(re.search("\d{1,2} (?:hr|min)$", arg)) and (
            self._flight_time is None
        ):
            self._flight_time = arg

        # number of stops
        elif ((arg == "Nonstop") or bool(re.search("\d stop", arg))) and (
            self._num_stops is None
        ):
            self._num_stops = 0 if arg == "Nonstop" else int(arg.split()[0])

        # co2
        elif arg.endswith("CO2") and (self._co2 is None):
            arg = arg.replace(",", "")
            self._co2 = int(arg.split()[0])

        # emissions
        elif arg.endswith("emissions") and (self._emissions is None):
            emission_val = arg.split()[0]
            self._emissions = 0 if emission_val == "Avg" else int(emission_val[:-1])

        # price
        elif arg.replace(",", "").isdigit() and (self._price is None):
            self._price = int(arg.replace(",", ""))

        # origin/dest
        elif (
            (len(arg) == 6 and arg.isupper() or "Flight + Train" in arg)
            and (self._origin is None)
            and (self._dest is None)
        ):
            if "Flight + Train" in arg:
                self._origin = self._queried_orig
                self._dest = self._queried_dest
                self._has_train = True
            else:
                self._origin = arg[:3]
                self._dest = arg[3:]

        # layover
        # regex 1: matches "FCO, JFK, ABC, DEF", "5 min Ancona", "3 hr 13 min FCO", "FCO, JFK"
        elif (
            bool(re.search("\d{0,2} (?:min|hr) (\d{0,2} (?:min|hr))?\w+", arg))
            and self._stops_locations is None
        ):
            # get stops locations
            if "," in arg:  # multiple stops
                self._stops_locations = arg
            else:  # single stop
                self._stops_locations = arg.split(" ")[-1]

            # get stops time
            if "," in arg:
                self._stops = arg.split(", ")[0]
            else:
                self._stops = (
                    re.search("([0-9]+ hr )?([0-9]+ min )?", arg).group().strip()
                )

        # airline
        elif len(arg) > 0 and (self._airline is None):
            if "Operated" in arg:
                airline = arg.split("Operated")[0]
            else:
                airline = arg

            # split camel case
            airline = re.sub("([a-z])([A-Z])", r"\1, \2", airline)

            # make it into an array (list)
            airline = airline.split(", ")

            self._airline = airline

        # other (trash)
        else:
            self._trash += [arg]

        # if we have both arrival and departure time, set them
        if len(self._times) == 2:
            self._time_leave = self._times[0]
            self._time_arrive = self._times[1]

    def _parse_args(self, args):
        for arg in args:
            self._classify_arg(arg)

    @staticmethod
    def convert_duration_str_to_minutes(s):
        """
        Returns the duration in minutes from a string of the form:
        3 hr 20 min --> 60*3 + 20 = 200
        20 min --> 20
        5 hr 55 min --> 60*5 + 55 = 355
        """
        if s is None or not bool(re.search("hr|min", str(s))):
            return None

        h = 0
        m = 0

        if "hr" in s:
            h = int(s.split("hr")[0])
        if "min" in s:
            m = int(re.split("hr|min", s)[-2])

        return 60 * h + m

    @staticmethod
    def make_dataframe(flights):
        """
        Generate a dataframe from lists of flight data
        """
        # create a dictionary with empty lists
        data = {
            "departure_datetime": [],
            "arrival_datetime": [],
            "airlines": [],
            "travel_time": [],
            "origin": [],
            "destination": [],
            "layover_n": [],
            "layover_time": [],
            "layover_location": [],
            "price_eur": [],
            "price_trend": [],
            "price_value": [],
            "access_date": [],
            "one_way": [],
            "has_train": [],
            "days_advance": [],
        }

        # populate the dictionary
        for flight in flights:
            data["departure_datetime"] += [flight._time_leave]
            data["arrival_datetime"] += [flight._time_arrive]
            data["airlines"] += [flight._airline]
            data["travel_time"] += [Flight.convert_duration_str_to_minutes(flight._flight_time)]
            data["origin"] += [flight._origin]
            data["destination"] += [flight._dest]
            data["layover_n"] += [flight._num_stops]
            data["layover_time"] += [Flight.convert_duration_str_to_minutes(flight._stops)]
            data["layover_location"] += [flight._stops_locations]
            data["price_eur"] += [flight._price]
            data["price_trend"] += [flight._price_trend[0]]
            data["price_value"] += [flight._price_trend[1]]
            data["access_date"] += [datetime.today()]
            data["one_way"] += [(False if flight._roundtrip else True)]
            data["has_train"] += [flight._has_train]
            data["days_advance"] += [(flight._time_leave - datetime.today()).days]

        return pd.DataFrame(data)

    # @staticmethod
    # def export_to_csv(df, origin, dest, date_leave, date_return=None):
    #     """
    #     Format:
    #     {access_date_YYMMDD}_{access_time_HHMM}_{orig}_{dest}_{days_advance}_{leave_date_YYMMDD}_{return_date_YYMMDD}
    #     """
    #     folder = "scrapes_csv"
    #     folder = path.join(path.dirname(__file__), folder)
    #     print("folder is", folder)

    #     # check if output folder exists
    #     if not path.isdir(folder):
    #         raise FileNotFoundError(f"Check if folder {folder} esists")

    #     access_date = df["access_date"][0].to_pydatetime().strftime("%y%m%d_%H%M")
    #     days_in_advance = df["days_advance"].min()
    #     leave_date = datetime.strptime(date_leave, "%Y-%m-%d").strftime("%y%m%d")
    #     return_date = (datetime.strptime(date_return, "%Y-%m-%d").strftime("%y%m%d") if date_return else None)

    #     res = f"{access_date}_{origin}_{dest}"
    #     res += f"_{leave_date}_{days_in_advance}"
    #     if return_date:
    #         res += f"_{return_date}"
    #     res += ".csv"

    #     full_filepath = path.join(folder, res)

    #     df.to_csv(full_filepath, index=False)
