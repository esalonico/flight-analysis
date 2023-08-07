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
        self._layover_n = None
        self._layover_time = None
        self._layover_location = None
        self._price = None
        self._price_trend = price_trend
        self._times_departure_arrival = []
        self._time_departure = None
        self._time_arrival = None
        self._has_train = False
        self._trash = []
        self._separate_tickets = False

        # extract the values above from the scraped HTML page source
        self._parse_args(*args)

    def _debug(self):
        res = {
            "origin": self._origin,
            "dest": self._dest,
            "airline": self._airline,
            "flight_time": self._flight_time,
            "layover_n": self._layover_n,
            "layover_time": self._layover_time,
            "layover_location": self._layover_location,
            "price": self._price,
            "price_trend": self._price_trend,
            "times_departure_arrival": self._times_departure_arrival,
            "time_departure": self._time_departure,
            "time_arrival": self._time_arrival,
            "has_train": self._has_train,
            "trash": self._trash,
        }
        return res

    def __repr__(self):
        return f"{self._origin}-{self._dest}-{self._date}"

    def _is_arg_layover(self, arg):
        """
        Returns True if the argument contains a layover location/time
        """
        # case 0: manually exclude "ITA" (it's a company name, not a layover location)
        if "ITA, " in arg:
            return False

        # layover location cases
        layover_location_cases = {
            # case 1: (xx hr xx min AAA), (xx hr xx min Aaaaa)
            1: re.search("\d{0,2} hr \d{0,2} min [A-Z]+", arg),
            # case 2: (xx hr AAA), (xx hr AAA)
            2: re.search("\d{0,2} hr [A-Z]+", arg),
            # case 3: (xx min AAA), (xx min AAA)
            3: re.search("\d{0,2} min [A-Z]+", arg),
            # case 4: (AAA, BBB, ...)
            4: re.search("^[A-Z]{3}, ([A-Z]{3}(, )?)?", arg),
        }

        return any(layover_location_cases.values())

    def _is_arg_airline(self, arg):
        if self._airline is None:
            return True
        return False

    def _is_arg_departure_arrival_times(self, arg):
        # regex: AM/PM (for example: 10:30AM, 4:11PM, 10:44AM+1)
        re_match = re.compile(r"\d{1,2}:\d{2}(?:AM|PM)(?:\+\d{0,1})?")

        if re_match.fullmatch(arg) and len(self._times_departure_arrival) < 2:
            return True

        return False

    def _is_arg_flight_time(self, arg):
        # regex:  3 hr 35 min, 45 min, 5 hr
        re_match = re.compile(r"^(?:\d{1,2} hr){0,1}\s{0,1}(?:\d{1,2} min){0,1}")

        if re_match.fullmatch(arg) and self._flight_time is None:
            return True

        return False

    def _is_arg_layover_n(self, arg):
        re_match = re.compile(r"\d stops{0,1}")

        if (arg == "Nonstop" or re_match.fullmatch(arg)) and self._layover_n is None:
            return True

        return False

    def _is_arg_price(self, arg):
        if arg.replace(",", "").isdigit() and self._price is None:
            return True
        return False

    def _is_arg_orig_dest(self, arg):
        # example: MUCFCO, BCNMAD
        if len(arg) == 6 and arg.isupper():
            return True
        return False

    # ---------------------------------------------------------------

    def _parse_departure_arrival_times(self, arg):
        dep, arr = (None, None)

        # handle + - days
        # extract the optional delta value (in case of +- X days)
        delta_days = int(arg[-1]) if arg[-2] == "+" else 0
        delta = timedelta(days=delta_days)

        # remove the delta value from the argument if present
        if delta_days:
            arg = arg[:-2]  # from 10:30PM+1 to 10:30PM

        # Combine date and time using a formatted string
        date_time_str = f"{self._date} {arg}"
        date_format = "%Y-%m-%d %I:%M%p"

        # Parse the date and time and add to the list
        date_ok = datetime.strptime(date_time_str, date_format) + delta
        self._times_departure_arrival.append(date_ok)

        if len(self._times_departure_arrival) != 2:
            return (None, None)

        return tuple(self._times_departure_arrival)

    def _parse_layover_times_location(self, arg):
        """
        From an argument (arg), returns the layover time and location as a tuple
        """
        layover_time = None
        layover_location = None

        # layover time
        if (" hr" in arg) or (" min" in arg):
            layover_time = (
                re.search("^(\d{1,2} hr){0,1}\s{0,1}(\d{1,2} min){0,1}\s", arg)
                .group()
                .strip()
            )
            layover_time = Flight.convert_duration_str_to_timedelta(layover_time)
            layover_location = [arg.split(" ")[-1]]

        # layover location
        if "," in arg:
            layover_location = arg.split(", ")
            layover_location = [x.strip() for x in layover_location]

        return layover_time, layover_location

    def _parse_airline(self, arg):
        airline = None
        dont_split = ["easyjet"]
        if "Operated" in arg:
            airline = arg.split("Operated")[0]
        else:
            airline = arg

        # split camel case
        if airline.lower() not in dont_split:
            airline = re.sub("([a-z])([A-Z])", r"\1, \2", airline)

        # make it into an array (list)
        airline = airline.split(", ")

        return airline

    def _parse_flight_time(self, arg):
        return Flight.convert_duration_str_to_timedelta(arg)

    def _parse_layover_n(self, arg):
        return 0 if arg == "Nonstop" else int(arg.split()[0])

    def _parse_price(self, arg):
        return int(arg.replace(",", ""))

    def _parse_orig_dest(self, arg):
        # special case: "Flight + Train"
        if "Flight + Train" in arg:
            self._has_train = True
            return (self._queried_orig, self._queried_dest)

        # regular case: like MUCFCO, LAXJFK
        return (arg[:3], arg[3:])

    # ---------------------------------------------------------------

    def _classify_arg(self, arg: str):
        """
        Classifies a string (arg) into the correct attribute for a flight,
        such as price, numer of layover stops, arrival time...
        """
        parsed = False

        # define cases for which to return early
        arg_empty = arg is None or arg == "" or len(arg) == 0
        arg_useless = arg in ["Change of airport", "round trip", "Climate friendly"]
        arg_delay = arg.startswith("Delayed")
        emissions = arg.endswith("emissions") or arg.endswith("CO2")

        early_return_conditions = [arg_empty, arg_useless, arg_delay, emissions]

        # return early
        if any(early_return_conditions):
            return

        # airline: takes care of special case of format:
        # Separate tickets booked together easyJet, Scoot
        # Separate tickets booked together Ryanair, SWISS, ITA
        if arg == "Separate tickets booked together":
            self._separate_tickets = True
            return

        if self._separate_tickets and "," in arg:
            self._separate_tickets = False
            self._airline = arg.split(", ")
            return

        # departure and arrival times
        if self._is_arg_departure_arrival_times(arg):
            (
                self._time_departure,
                self._time_arrival,
            ) = self._parse_departure_arrival_times(arg)
            return

        # flight time
        if self._is_arg_flight_time(arg):
            self._flight_time = self._parse_flight_time(arg)
            return

        # number of stops (layover n)
        if self._is_arg_layover_n(arg):
            self._layover_n = self._parse_layover_n(arg)
            return

        # price
        if self._is_arg_price(arg):
            self._price = self._parse_price(arg)
            return

        # origin and destination airports
        if self._is_arg_orig_dest(arg):
            self._origin, self._dest = self._parse_orig_dest(arg)
            return

        # layover time and location(s)
        if self._is_arg_layover(arg):
            (
                self._layover_time,
                self._layover_location,
            ) = self._parse_layover_times_location(arg)
            return

        # airline (always have it at last since it captures everything not captured above)
        if self._is_arg_airline(arg):
            self._airline = self._parse_airline(arg)
            return

    def _parse_args(self, args):
        # don't process if there are not enough arguments
        if len(args) > 5:
            for arg in args:
                self._classify_arg(arg)

            # print(self._debug())

    @staticmethod
    def convert_duration_str_to_timedelta(s):
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

        return timedelta(hours=h, minutes=m)

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
            # "days_advance": [],
        }

        # populate the dictionary
        for flight in flights:
            try:
                data["departure_datetime"] += [flight._time_departure]
                data["arrival_datetime"] += [flight._time_arrival]
                data["airlines"] += [flight._airline]
                data["travel_time"] += [flight._flight_time]
                data["origin"] += [flight._origin]
                data["destination"] += [flight._dest]
                data["layover_n"] += [flight._layover_n]
                data["layover_time"] += [flight._layover_time]
                data["layover_location"] += [flight._layover_location]
                data["price_eur"] += [flight._price]
                data["price_trend"] += [flight._price_trend[0]]
                data["price_value"] += [flight._price_trend[1]]
                data["access_date"] += [datetime.today()]
                data["one_way"] += [(False if flight._roundtrip else True)]
                data["has_train"] += [flight._has_train]
                # data["days_advance"] += [
                #     (flight._time_departure - datetime.today()).days
                # ]
            except Exception as e:
                print("Error with flight", flight, flight._price)
                print(e)

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
