# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm
import re
from os import path

__all__ = ['Flight']


class Flight:

    def __init__(self, dl, roundtrip, price_trend, *args):
        self._roundtrip = roundtrip
        self._id = 1
        self._origin = None
        self._dest = None
        self._date = dl
        self._dow = datetime.strptime(dl, '%Y-%m-%d').isoweekday() # day of week
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
        self._trash = []
        self._parse_args(*args)

    def __repr__(self):
        return f"{self._id}-{self._origin}-{self._dest}-{self._date}"

    @property
    def roundtrip(self):
        return self._roundtrip
    
    @property
    def id(self):
        return self._id

    @property
    def origin(self):
        return self._origin

    @origin.setter
    def origin(self, x : str) -> None:
        self._origin = x

    @property
    def dest(self):
        return self._dest

    @dest.setter
    def dest(self, x : str) -> None:
        self._dest = x

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, x : str) -> None:
        self._date = x

    @property
    def dow(self):
        return self._dow

    @property
    def airline(self):
        return self._airline

    @property
    def flight_time(self):
        return self._flight_time

    @property
    def num_stops(self):
        return self._num_stops

    @property
    def stops(self):
        return self._stops

    @property
    def stops_locations(self):
        return self._stops_locations

    @property
    def co2(self):
        return self._co2

    @property
    def emissions(self):
        return self._emissions

    @property
    def price(self):
        return self._price
    
    @property
    def price_trend(self):
        return self._price_trend

    @property
    def time_leave(self):
        return self._time_leave

    @property
    def time_arrive(self):
        return self._time_arrive


    def _classify_arg(self, arg: str):
        """
        Classifies a string (arg) into the correct attribute for a flight,
        such as price, numer of layover stops, arrival time...
        """
        
        # handle emptu strings
        if arg is None or arg == "":
            return
        
        # handle special cases
        if arg == "Change of airport":
            self._stops = self._stops_locations = "Change of airport"
            return
        elif arg in ["round trip", "Climate friendly"]:
            return

        # arrival or departure time
        # regex: AM/PM (for example: 10:30AM, 4:11PM)
        if bool(re.search("\d{1,2}\:\d{2}(?:AM|PM)\+{0,1}\d{0,1}", arg)) and (len(self._times) < 2):
            delta = timedelta(days = 0)
            if arg[-2] == '+':
                delta = timedelta(days = int(arg[-1]))
                arg = arg[:-2]

            date_format = "%Y-%m-%d %I:%M%p"
            self._times += [datetime.strptime(self._date + " " + arg, date_format) + delta]

        # flight time       
        # regex:  3 hr 35 min, 45 min, 5 hr
        elif bool(re.search("\d{1,2} (?:hr|min)$", arg)) and (self._flight_time is None):
            self._flight_time = arg
   
        # number of stops
        elif ((arg == "Nonstop") or bool(re.search("\d stop", arg))) and (self._num_stops is None):
            self._num_stops = (0 if arg == 'Nonstop' else int(arg.split()[0]))

        # co2 
        elif arg.endswith('CO2') and (self._co2 is None):
            self._co2 = int(arg.split()[0])
        
        # emissions
        elif arg.endswith('emissions') and (self._emissions is None):
            emission_val = arg.split()[0]
            self._emissions = 0 if emission_val == 'Avg' else int(emission_val[:-1])
        
        # price
        elif arg.replace(',','').isdigit() and (self._price is None):
            self._price = int(arg.replace(',',''))
   
        # origin/dest        
        elif (len(arg) == 6 and arg.isupper() or "Flight + Train" in arg) and (self._origin is None) and (self._dest is None):
            if "Flight + Train" in arg:
                self._origin = self._dest = "Flight + Train"
            else:
                self._origin = arg[:3]
                self._dest = arg[3:]

        # layover
        # regex 1: matches "FCO, JFK, ABC, DEF", "5 min Ancona", "3 hr 13 min FCO", "FCO, JFK"
        elif bool(re.search("\d{0,2} (?:min|hr) (\d{0,2} (?:min|hr))?\w+", arg)) and self._stops_locations is None:
            # get stops locations
            if "," in arg: # multiple stops
                self._stops_locations = arg
            else: # single stop
                self._stops_locations = arg.split(" ")[-1]
                
            # get stops time
            if "," in arg:
                self._stops = arg.split(", ")[0]
            else:
                self._stops = re.search("([0-9]+ hr )?([0-9]+ min )?", arg).group().strip()
                
        
        # airline
        elif len(arg) > 0 and (self._airline is None):
            if "Operated" in arg:
                airline = arg.split("Operated")[0]
            else:
                airline = arg
    
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
    def get_duration_from_string(s):
        """
        Returns a better formatted string for a duration element.
        For example:
        3 hr 20 min --> 03:20
        """
        if not bool(re.search("hr|min", str(s))):
            return s
        
        h = 0
        m = 0
        
        if "hr" in s:
            h = int(s.split("hr")[0])
        if "min" in s:
            m = int(re.split("hr|min", s)[-2])

        # return timedelta(hours=h, minutes=m)
        return f"{h:02d}:{m:02d}"
        
    
    @staticmethod
    def dataframe(flights):
        """
        Generate a dataframe from lists of flight data
        """
        data = {
            'departure_datetime': [],
            'arrival_datetime': [],
            'airlines' : [],
            'travel_time' : [],
            'origin' : [],
            'destination' : [],
            'layover_n' : [],
            'layover_time' : [],
            'layover_location' : [],
            'price_eur' : [],
            'price_trend' : [],
            'price_value' : [],
            'access_date' : [],
            'flight_type' : []
        }

        for flight in flights:
            data['departure_datetime'] += [flight.time_leave]
            data['arrival_datetime'] += [flight.time_arrive]
            data['airlines'] += [flight.airline]
            data['travel_time'] += [flight.flight_time]
            data['origin'] += [flight.origin]
            data['destination'] += [flight.dest]
            data['layover_n'] += [flight.num_stops]
            data['layover_time'] += [flight.stops]
            data['layover_location'] += [flight.stops_locations]
            data['price_eur'] += [flight.price]
            data["price_trend"] += [flight.price_trend[0]]
            data["price_value"] += [flight.price_trend[1]]
            data['access_date'] += [datetime.today()]
            data['flight_type'] += [("roundtrip" if flight.roundtrip else "oneway")]
            
        df = pd.DataFrame(data)
        
        # further cleaning
		# convert: travel time to duration
        df['travel_time'] = df['travel_time'].apply(lambda x: Flight.get_duration_from_string(x))
        df['layover_time'] = df['layover_time'].apply(lambda x: Flight.get_duration_from_string(x))
        
        # add column: Days in Advance
        df['days_advance'] = (df['departure_datetime'] - df['access_date']).dt.days
        
        return df
    
    @staticmethod
    def export_to_csv(df, origin, dest, date_leave, date_return=None):
        """
        Format:
        {access_date_YYMMDD}_{access_time_HHMM}_{orig}_{dest}_{days_advance}_{leave_date_YYMMDD}_{return_date_YYMMDD}
        """
        folder = "outputs"
        
        # check if output folder exists
        if not path.isdir(folder):
            raise FileNotFoundError(f"Check if folder {folder} esists")
    
        access_date = datetime.strptime(df["access_date"][0], "%Y-%m-%d %H:%M:%S").strftime("%y%m%d_%H%M")
        days_in_advance = df["days_advance"].min()
        leave_date = datetime.strptime(date_leave, "%Y-%m-%d").strftime("%y%m%d")
        return_date = (datetime.strptime(date_return, "%Y-%m-%d").strftime("%y%m%d") if date_return else None)

        res = f"{access_date}_{origin}_{dest}"
        res += f"_{leave_date}_{days_in_advance}"
        if return_date:
            res += f"_{return_date}"
        res += ".csv"
        
        full_filepath = path.join(folder, res)
        
        # if file already exists, raise ValueError
        if path.isfile(full_filepath):
            print(f"File {full_filepath} already exists, overwriting...")
        
        df.to_csv(full_filepath, index=False)
