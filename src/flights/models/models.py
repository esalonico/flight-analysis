import json
from datetime import date
from typing import List, Optional

from pydantic import BaseModel, computed_field

DATA_FOLDER = "src/flights/data"

with open(f"{DATA_FOLDER}/airports.json", "r") as f:
    AIRPORTS_DATA = json.load(f)


class Airport(BaseModel):
    iata: str
    icao_code: Optional[str] = None
    type: Optional[str] = None
    name: Optional[str] = None
    continent: Optional[str] = None
    iso_country: Optional[str] = None
    iso_region: Optional[str] = None
    municipality: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None

    def __init__(self, iata: str):
        if iata not in AIRPORTS_DATA:
            raise ValueError(f"No data available for IATA code '{iata}'")

        airport_data = AIRPORTS_DATA[iata]
        super().__init__(iata=iata, **airport_data)

    def __repr__(self) -> str:
        return self.iata

    @classmethod
    def get_all_iatas(self) -> List[str]:
        return list(AIRPORTS_DATA.keys())

    @classmethod
    def get_all_iatas_with_names(self) -> List[str]:
        return [f"{k} ({v['name']})" for k, v in AIRPORTS_DATA.items()]


class SingleSearch(BaseModel):
    """Single search item (one origin, one destination, one departure date)"""

    origin: Airport
    destination: Airport
    departure_date: date
    direct_only: bool


class CompositeSearch(BaseModel):
    """
    Collection of search items.
    Multiple origins, destinations, and departure dates can be combined.
    """

    origins: Optional[List[Airport]] = None
    destinations: Optional[List[Airport]] = None
    departure_dates: Optional[List[date]] = None
    direct_only: Optional[bool] = None

    @computed_field
    @property
    def single_searches(self) -> Optional[List[SingleSearch]]:
        if self.origins is None or self.destinations is None or self.departure_dates is None:
            return None

        return [
            SingleSearch(
                origin=origin,
                destination=destination,
                departure_date=departure_date,
                direct_only=self.direct_only,
            )
            for origin in self.origins
            for destination in self.destinations
            for departure_date in self.departure_dates
        ]
