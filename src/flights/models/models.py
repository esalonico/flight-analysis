import json
from datetime import date
from typing import List, Optional

from pydantic import BaseModel

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


class SearchParameters(BaseModel):
    origins: Optional[List[Airport]] = None
    destinations: Optional[List[Airport]] = None
    departure_dates: Optional[List[date]] = None
