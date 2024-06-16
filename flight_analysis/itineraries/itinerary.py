import pandas as pd

from flight_analysis.scrapers.base_scraper import BaseScraper
from flight_analysis.scrapers.search_query import SearchQuery
from flight_analysis.utils import utils


class BaseItinerary:
    def __init__(self, scraper: BaseScraper):
        self.scraper = scraper

    def make_itinerary_df(self):
        raise NotImplementedError("This method must be implemented in a subclass.")

    def compute_flight_leg_within_itinerary(self, sq: SearchQuery, flight: pd.Series) -> str:
        """
        In a given itinerary, compute the flight leg (either departing or returning) that a given flight belongs to.
        """
        if flight["airport_dep"] == sq.airport_dep.iata:
            return "departing"
        elif flight["airport_dep"] == sq.airport_arr.iata:
            return "returning"
        else:
            return "unknown"


class DirectOneWayItinerary(BaseItinerary):
    def __init__(self, scraper: BaseScraper):
        super().__init__(scraper)

    def make_itinerary_df(self):
        flights_df = self.scraper.make_flights_df()

        itinerary_df = flights_df.copy()
        itinerary_df["option"] = range(1, len(flights_df) + 1)

        # move option column to the front
        itinerary_df = utils.move_pandas_column_to_front(itinerary_df, "option")

        return itinerary_df


class ReturnItinerary(BaseItinerary):
    def __init__(self, scraper: BaseScraper):
        super().__init__(scraper)

    def make_itinerary_df(self) -> pd.DataFrame:
        if not self.scraper.flights or len(self.scraper.flights) == 0:
            return

        flights_df = self.scraper.make_flights_df()

        df = flights_df.copy()
        df["leg"] = df.apply(lambda x: self.compute_flight_leg_within_itinerary(self.scraper.search_query, x), axis=1)

        # group by flight combination and sort by leg
        df = (
            df.groupby("flight_combination", group_keys=True)
            .apply(lambda x: x.sort_values(["leg", "price", "duration"], ascending=[True, True, True]))
            .reset_index(drop=True)
        )

        # rename flight_combination with option
        df = df.rename(columns={"flight_combination": "option"})

        # move option and leg column to the front
        df = utils.move_pandas_column_to_front(df, "leg")
        df = utils.move_pandas_column_to_front(df, "option")

        return df


class LayoverOneWayItinerary(BaseItinerary):
    def __init__(self, scraper: BaseScraper):
        super().__init__(scraper)

    def make_itinerary_df(self):
        flights_df = self.scraper.make_flights_df()

        itinerary_df = flights_df.copy()
        itinerary_df["option"] = range(1, len(flights_df) + 1)

        # move option column to the front
        itinerary_df = utils.move_pandas_column_to_front(itinerary_df, "option")

        return itinerary_df
