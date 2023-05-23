# Inspired and adapted from https://pypi.org/project/google-flight-analysis/
# author: Emanuele Salonico, 2023

from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd
from tqdm import tqdm
import re

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


    def _classify_arg(self, arg):
        if arg == "Change of airport":
            self._stops = self._stops_locations = "Change of airport"
            return
        elif arg in ["round trip", "Climate friendly"]:
            return

        # arrival or departure time
        if ('AM' in arg or 'PM' in arg) and len(self._times) < 2:
            delta = timedelta(days = 0)
            if arg[-2] == '+':
                delta = timedelta(days = int(arg[-1]))
                arg = arg[:-2]

            date_format = "%Y-%m-%d %I:%M%p"
            self._times += [datetime.strptime(self._date + " " + arg, date_format) + delta]

        # flight time        
        elif ('hr' in arg or 'min'in arg) and self._flight_time is None:
            self._flight_time = arg
   
        # number of stops
        elif 'stop' in arg and self._num_stops is None:
            self._num_stops = 0 if arg == 'Nonstop' else int(arg.split()[0])

        # co2 
        elif arg.endswith('CO2') and self._co2 is None:
            self._co2 = int(arg.split()[0])
        
        # emissions
        elif arg.endswith('emissions') and self._emissions is None:
            emission_val = arg.split()[0]
            self._emissions = 0 if emission_val == 'Avg' else int(emission_val[:-1])
        
        # price
        elif arg.replace(',','').isdigit() and self._price is None:
            self._price = int(arg.replace(',',''))
   
        # origin/dest        
        elif (len(arg) == 6 and arg.isupper() or "Flight + Train" in arg) and (self._origin is None) and (self._dest is None):
            if "Flight + Train" in arg:
                self._origin = self._dest = "Flight + Train"
            else:
                self._origin = arg[:3]
                self._dest = arg[3:]

        # layover (1 stop + time at stop or multiple stops)
        elif (('hr' in arg or "min" in arg)) or (len(arg.split(', ')) > 1 and arg.isupper()):
            # stops locations: 1 location
            if (arg[-3:].isupper() or arg.split(" ")[-1][0].isupper()) and ("," not in arg):
                self._stops = arg[:-3]
                if arg[-3:].isupper():
                    self._stops_locations = arg[-3:]
                else:
                    self._stops_locations = arg.split(" ")[-1]
            # 2 or more
            else:
                self._stops = arg
                self._stops_locations = arg
        
        # airline
        elif len(arg) > 0:
            if "Operated" in arg:
                airline = arg.split("Operated")[0]
            else:
                airline = arg
    
            self._airline = airline
        
        # other
        else:
            self._trash += [arg]
            # airline and other stuff idk

        if len(self._times) == 2:
            self._time_leave = self._times[0]
            self._time_arrive = self._times[1]

    def _parse_args(self, args):
        for arg in args:
            self._classify_arg(arg)

    @staticmethod
    def get_duration_from_string(s):
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
        data = {
            'Departure datetime': [],
            'Arrival datetime': [],
            'Airline(s)' : [],
            'Travel Time' : [],
            'Origin' : [],
            'Destination' : [],
            'Num Stops' : [],
            'Layover' : [],
            'Stops Location' : [],
            # 'CO2 Emission (kg)' : [],
            # 'Emission Diff (%)' : [],
            'Price (€)' : [],
            'Price Trend' : [],
            'Price Value' : [],
            'Access Date' : [],
            'Flight Type' : []
            
        }

        for flight in flights:
            data['Departure datetime'] += [flight.time_leave]
            data['Arrival datetime'] += [flight.time_arrive]
            data['Airline(s)'] += [flight.airline]
            data['Travel Time'] += [flight.flight_time]
            data['Origin'] += [flight.origin]
            data['Destination'] += [flight.dest]
            data['Num Stops'] += [flight.num_stops]
            data['Layover'] += [flight.stops]
            data['Stops Location'] += [flight.stops_locations]
            # data['CO2 Emission (kg)'] += [flight.co2]
            # data['Emission Diff (%)'] += [flight.emissions]
            data['Price (€)'] += [flight.price]
            data["Price Trend"] += [flight.price_trend[0]]
            data["Price Value"] += [flight.price_trend[1]]
            data['Access Date'] += [datetime.today()]
            data['Flight Type'] += [("Roundtrip" if flight.roundtrip else "One Way")]
            
        df = pd.DataFrame(data)
        
        # further cleaning
		# convert: travel time to duration
        df['Travel Time'] = df['Travel Time'].apply(lambda x: Flight.get_duration_from_string(x))
        df['Layover'] = df['Layover'].apply(lambda x: Flight.get_duration_from_string(x))
        
        # add column: Days in Advance
        df['Days in Advance'] = (df['Departure datetime'] - df['Access Date']).dt.days
        
        # format column: Access Date
        df['Access Date'] = df['Access Date'].dt.strftime("%Y-%m-%d %H:%M:%S")

        return df
     
    @staticmethod
    def assert_error(x, arg): # TODO: this is outdated
        return [
            "Parsing Arg 0 as Date Leave elem is incorrect.",
            "Parsing Arg 1 as Date Return elem is incorrect.",
            -1,
            -1,
            -1,
            "Parsing Arg 6 as num stop elem is incorrect."
            "Parsing Arg 7 as CO2 elem is incorrect.",
            "Parsing Arg 8 as emissions elem is incorrect.",
            "Parsing Arg 9 as price elem is incorrect."
        ][x] + ": " + arg 
