import pandas as pd
import os
from backend.utils.sheets_downloader import _download_airports_sheet


class Airport:
    _airports_df_filepath = "backend/data/sheets/airports.csv"
    if not os.path.isfile(_airports_df_filepath):
        _download_airports_sheet(force_download=True)
    
    _airports_df = pd.read_csv(_airports_df_filepath, index_col=0)

    def __init__(self, iata) -> None:
        self.iata = self._check_airport_exists(iata)
        self.name = self._get_airport_name(iata)
        self.city = self._get_airport_city(iata)
        self.country = self._get_airport_country(iata)
        self.type = self._get_airport_type(iata)
        self.lat = self._get_airport_lat(iata)
        self.lon = self._get_airport_lon(iata)

    def __repr__(self) -> str:
        return f"Airport({self.iata}, {self.name}, {self.city}, {self.country})"

    def __str__(self) -> str:
        return self.iata

    @classmethod
    def _check_airport_exists(cls, iata_code: str) -> str:
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
