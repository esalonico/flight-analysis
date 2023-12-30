import os

import pandas as pd


class Airport:
    
    def __init__(self, iata) -> None:
        self._airports_df_filepath = os.path.join(os.path.dirname(__file__), "sheets", "airports.csv")
        self._airports_df = pd.read_csv(self._airports_df_filepath, index_col=0)
        
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
    


    def _check_airport_exists(self, iata_code: str) -> str:
        """
        Checks if the airport exists in the airport dataframe.
        Returns: IATA code of the airport.
        """
        if len(iata_code) != 3 or iata_code not in self._airports_df.index:
            raise ValueError(f"{iata_code} is not a valid airport code.")
        
        return iata_code
    
    def _get_airport_name(self, iata_code: str) -> str:
        """
        Returns the name of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "name"]
    
    def _get_airport_country(self, iata_code: str) -> str:
        """
        Returns the country of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "iso_country"]
    
    def _get_airport_city(self, iata_code: str) -> str:
        """
        Returns the city of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "municipality"]
    
    def _get_airport_type(self, iata_code: str) -> str:
        """
        Returns the type of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "type"]
    
    def _get_airport_lat(self, iata_code: str) -> float:
        """
        Returns the latitude of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "lat"]
    
    def _get_airport_lon(self, iata_code: str) -> float:
        """
        Returns the longitude of the airport from the dataframe.
        """
        return self._airports_df.loc[iata_code, "lon"]