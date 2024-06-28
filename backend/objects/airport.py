import logging
import os
from collections import deque

import pandas as pd

import backend.utils.sheets_downloader as sheets_downloader
from backend.utils.utils import setup_logging

setup_logging()
logger = logging.getLogger(os.path.basename(__file__))


class Airport:
    _airports_df_filepath = "backend/data/sheets/airports.csv"
    _airports_flightconnections_codes_filepath = "backend/data/sheets/airports_flightconnections.csv"
    _airports_flightconnections_connections_filepath = "backend/data/sheets/connections_flightconnections.csv"

    if not os.path.isfile(_airports_df_filepath):
        sheets_downloader._download_airports_sheet(force_download=True)

    if not os.path.isfile(_airports_flightconnections_codes_filepath):
        sheets_downloader._download_flightconnections_airport_codes_sheet(force_download=True)

    if not os.path.isfile(_airports_flightconnections_connections_filepath):
        sheets_downloader._download_flightconnections_airport_connections_sheet(force_download=True)

    _airports_df = pd.read_csv(_airports_df_filepath, index_col=0)
    _flightconnections_airport_codes_df = pd.read_csv(_airports_flightconnections_codes_filepath, index_col=0)
    _flightconnections_airport_connections_df = pd.read_csv(_airports_flightconnections_connections_filepath)
    _aiports_connections_graph = _flightconnections_airport_connections_df.groupby("iata_code_from")["iata_code_to"].apply(list).to_dict()

    def __init__(self, iata):
        self.iata = self._validate_iata_code(iata)
        self.name = self._get_airport_name(iata)
        self.city = self._get_airport_city(iata)
        self.country = self._get_airport_country(iata)
        self.type = self._get_airport_type(iata)
        self.lat = self._get_airport_lat(iata)
        self.lon = self._get_airport_lon(iata)
        self.flightconnections_code = self._get_flightconnections_airport_code(iata)

        logger.debug(self.__repr__())

    def __repr__(self) -> str:
        return f"Airport({self.iata}, {self.name}, {self.city}, {self.country})"

    def __str__(self) -> str:
        return self.iata

    @classmethod
    def _validate_iata_code(cls, iata_code: str) -> str:
        """
        Checks if the airport exists in the airport dataframe.
        Returns: IATA code of the airport.
        """
        if len(iata_code) != 3 or iata_code not in cls._airports_df.index:
            raise ValueError(f"{iata_code} is not a valid airport code.")

        return iata_code

    @classmethod
    def _get_airport_name(cls, iata_code: str) -> str:
        """
        Returns the name of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "name"]

    @classmethod
    def _get_airport_country(cls, iata_code: str) -> str:
        """
        Returns the country of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "iso_country"]

    @classmethod
    def _get_airport_city(cls, iata_code: str) -> str:
        """
        Returns the city of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "municipality"]

    @classmethod
    def _get_airport_type(cls, iata_code: str) -> str:
        """
        Returns the type of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "type"]

    @classmethod
    def _get_airport_lat(cls, iata_code: str) -> float:
        """
        Returns the latitude of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "lat"]

    @classmethod
    def _get_airport_lon(cls, iata_code: str) -> float:
        """
        Returns the longitude of the airport from the dataframe.
        """
        return cls._airports_df.loc[iata_code, "lon"]

    @classmethod
    def _get_flightconnections_airport_code(cls, iata_code: str) -> int:
        """
        Returns the flightconnections airport code from the dataframe.
        """
        try:
            int(cls._flightconnections_airport_codes_df.loc[iata_code, "code"])
        except Exception as e:
            raise ValueError(f"Could not find flightconnections code for {iata_code}.") from e

    def get_all_connections(self, dest_iata: str, max_stops: int):
        """
        Returns all possible connections from the current airport to the destination airport within the given number of stops.

        :param dest_iata: IATA code of the destination airport.
        :param max_stops: Maximum number of stops allowed (1 = direct flight).

        :return: List of possible connections.
        """
        assert max_stops in [1, 2, 3], "Number of maximum stops must be 1 (direct), 2, or 3."
        assert self._validate_iata_code(dest_iata), f"{dest_iata} is not a valid airport code."

        # Use a queue to perform BFS
        queue = deque([(self.iata, [self.iata])])

        connections = []
        while queue:
            current_airport, path = queue.popleft()

            # If we reached the destination and within the max_stops limit
            if current_airport == dest_iata and len(path) - 1 <= max_stops:
                connections.append(path)

            # If we can still make more stops
            if len(path) - 1 < max_stops:
                for next_airport in self._aiports_connections_graph.get(current_airport, []):
                    if next_airport not in path:  # Avoid cycles
                        queue.append((next_airport, path + [next_airport]))

        logger.debug(f"Found {len(connections)} connections from {self.iata} to {dest_iata} with {max_stops} stops.")
        return connections
