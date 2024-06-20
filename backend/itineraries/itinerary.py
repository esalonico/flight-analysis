import pandas as pd

from backend.scrapers.base_scraper import BaseScraper
from backend.scrapers.search_query import SimpleSearchQuery
from backend.utils import utils


class BaseItinerary:
    def __init__(self, scraper: BaseScraper):
        self.scraper = scraper

    def make_itinerary_df(self):
        raise NotImplementedError("This method must be implemented in a subclass.")


class OneWayItinerary(BaseItinerary):
    def __init__(self, scraper: BaseScraper):
        super().__init__(scraper)

    def make_itinerary_df(self, export_to: str = None) -> pd.DataFrame:
        """
        Create a DataFrame with the flights information.

        :param export_to: Optional string with the filename to export the DataFrame to a CSV file.

        :return: DataFrame with the flights information.
        """
        if not self.scraper.flights or len(self.scraper.flights) == 0:
            return

        flights_df = self.scraper.make_flights_df()
        itinerary_df = flights_df.copy()

        # sort by number of stops and price
        itinerary_df = itinerary_df.sort_values(["price", "n_stops", "duration"], ascending=[True, True, True])

        # create option column
        itinerary_df["option"] = range(1, len(flights_df) + 1)
        itinerary_df = utils.move_pandas_column_to_front(itinerary_df, "option")

        # reset index
        itinerary_df = itinerary_df.reset_index(drop=True)

        # export to CSV if requested
        if export_to:
            itinerary_df.to_csv(export_to, index=False)

        return itinerary_df


class RoundTripItinerary(BaseItinerary):
    def __init__(self, scraper: BaseScraper):
        super().__init__(scraper)

    def compute_flight_leg_within_itinerary(self, sq: SimpleSearchQuery, flight: pd.Series) -> str:
        """
        In a given roundtrip itinerary, compute the flight leg (either departing or returning) that a given flight belongs to.

        :param sq: SimpleSearchQuery object with the search parameters
        :param flight: pd.Series representing a flight

        :return: String with the leg of the flight within the itinerary.
        """
        if flight["airport_dep"] == sq.airport_dep.iata:
            return "departing"
        elif flight["airport_dep"] == sq.airport_arr.iata:
            return "returning"
        else:
            return "unknown"

    def make_itinerary_df(self, export_to: str = None) -> pd.DataFrame:
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

        # export to CSV if requested
        if export_to:
            df.to_csv(export_to, index=False)

        return df
