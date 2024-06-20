from datetime import datetime

from fastapi import APIRouter, HTTPException

import backend.utils.utils as utils
from backend.itineraries.itinerary import OneWayItinerary
from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.search_query import SimpleSearchQuery

router = APIRouter()


@router.post("/itinerary/one-way")
def get_itinerary_one_way(search_data: dict):
    # check if all required keys are present in search_data
    required_keys = ["airport_from", "airport_to", "datetime_dep"]
    for key in required_keys:
        if key not in search_data:
            raise HTTPException(status_code=400, detail=f"Key {key} not found in search_data")

    # handle direct_only parameter
    if "direct_only" not in search_data:
        search_data["direct_only"] = True

    # validate and convert datetime_dep
    try:
        datetime_dep = datetime.strptime(search_data["datetime_dep"], "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format for datetime_dep. Expected format: YYYY-MM-DD")

    # scrape the data
    # TODO: handle airport objects if not found
    airport_from = Airport(search_data["airport_from"])
    airport_to = Airport(search_data["airport_to"])

    search_query = SimpleSearchQuery(airport_from, airport_to, datetime_dep)
    scraper = OneWayScraper(search_query, direct_only=search_data["direct_only"])
    scraper.scrape()

    itineary = OneWayItinerary(scraper)
    itineary_df = itineary.make_itinerary_df()

    return itineary_df.to_dict(orient="records")
