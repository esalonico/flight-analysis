import logging
import multiprocessing
import os
from datetime import date, datetime, timedelta
from typing import List

import pandas as pd
from tqdm import tqdm

from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SearchQuery, SingleItemSearchQuery
from backend.utils import utils
from backend.utils.utils import setup_logging

setup_logging()
logger = logging.getLogger(os.path.basename(__file__))


class BaseItinerary:
    def __init__(self, search_query: SearchQuery, direct_only: bool):
        self.search_query = search_query
        self.direct_only = direct_only

    def scrape(self):
        raise NotImplementedError("This method must be implemented in a subclass.")

    def make_itinerary_df(self):
        raise NotImplementedError("This method must be implemented in a subclass.")

    def _is_itinerary_multidate(self):
        return self.search_query.__class__.__name__ == "MultiDateSearchQuery"


class OneWayItinerary(BaseItinerary):
    def __init__(self, search_query, direct_only):
        super().__init__(search_query, direct_only)

    def scrape(self, export_to: str = None):
        combination_flights = []
        pbar = tqdm(self.search_query.combinations, position=1, leave=False)
        for combination in pbar:
            pbar.set_description(f"Scraping {combination}")
            scraper = OneWayScraper(combination, self.direct_only)
            scraper.scrape(combination)  # OneWayScraper.scrape()
            df = scraper.make_flights_df()
            if not df.empty and not df.isna().all(axis=None):
                combination_flights.append(df)

        if not combination_flights:
            self.df = pd.DataFrame()
            return

        flight_df = pd.concat(combination_flights)
        self.df = self.make_itinerary_df(flight_df, export_to)

    def make_itinerary_df(self, flights_df: pd.DataFrame, export_to: str = None) -> pd.DataFrame:
        """
        Create a DataFrame with the flights information.

        :param flights_df: DataFrame with the flights information.
        :param export_to: Optional string with the filename to export the DataFrame to a CSV file.

        :return: DataFrame with the flights information.
        """
        itinerary_df = flights_df.copy()

        if itinerary_df.empty:
            return pd.DataFrame()

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
    def __init__(self, search_query, direct_only):
        super().__init__(search_query, direct_only)

    def scrape(self, export_to: str = None):
        combination_flights = []
        pbar = tqdm(self.search_query.combinations)
        for combination in pbar:
            pbar.set_description(f"Scraping {combination}")
            scraper = RoundTripScraper(combination, self.direct_only)
            logger.debug(f"Scraping RoundTripScraper {combination}")
            scraper.scrape()  # RoundTripScraper.scrape()
            df = scraper.make_flights_df()
            if not df.empty and not df.isna().all(axis=None):
                combination_flights.append(df)

        if not combination_flights:
            self.df = pd.DataFrame()
            return

        flight_df = pd.concat(combination_flights)
        self.df = self.make_itinerary_df(flight_df, export_to)

    def make_itinerary_df(self, flights_df: pd.DataFrame, export_to: str = None) -> pd.DataFrame:
        """
        Create a DataFrame with the flights information.

        :param flights_df: DataFrame with the flights information.
        :param export_to: Optional string with the filename to export the DataFrame to a CSV file.

        :return: DataFrame with the flights information.
        """

        def compute_flight_leg_within_itinerary(sq: SearchQuery, flight: pd.Series) -> str:
            """
            In a given roundtrip itinerary, compute the flight leg (either departing or returning) that a given flight belongs to.

            :param sq: SearchQuery object with the search parameters
            :param flight: pd.Series representing a flight

            :return: String with the leg of the flight within the itinerary.
            """
            if flight["airport_dep"] in [a.iata for a in sq.airports_dep]:
                return "departing"
            elif flight["airport_dep"] in [a.iata for a in sq.airports_arr]:
                return "returning"
            else:
                return "unknown"

        df_in = flights_df.copy()

        # compute column for the flight leg (departing or returning)
        df_in["leg"] = df_in.apply(lambda x: compute_flight_leg_within_itinerary(self.search_query, x), axis=1)

        # assign each departing flight an ID of itself
        df_in["departing_flight_id"] = df_in["departing_flight_id"].fillna(df_in["_id"])

        # Compute column for the price rank within the departing flights to order them
        df_departing = df_in[df_in["leg"] == "departing"].copy()
        df_departing.loc[:, "departing_price_rank"] = df_departing.groupby("departing_flight_id")["price"].rank(method="dense")

        # Merge the ranks back into the original DataFrame
        df_in = df_in.merge(df_departing[["departing_flight_id", "_id", "departing_price_rank"]], on=["departing_flight_id", "_id"], how="left")

        # Step 1: Compute the sum of departing_price_rank for each group
        grouped_sum = df_in.groupby("departing_flight_id")["departing_price_rank"].sum().reset_index()

        # Step 2: Sort the groups based on the sum of departing_price_rank
        grouped_sum_sorted = grouped_sum.sort_values(by="departing_price_rank")

        # Step 3: Define a custom sorting function that sorts within each group by leg
        def custom_sort(df):
            return df.sort_values(by="leg", ascending=True)

        # Step 4: Apply the custom sorting function and concatenate the sorted groups with "option" column
        sorted_groups = []
        for i, group_id in enumerate(grouped_sum_sorted["departing_flight_id"], start=1):
            sorted_group = custom_sort(df_in[df_in["departing_flight_id"] == group_id])
            sorted_group["option"] = i
            sorted_groups.append(sorted_group)

        sorted_df = pd.concat(sorted_groups).reset_index(drop=True)

        # clean up dataframe
        sorted_df = sorted_df.drop(columns=["_id", "flight_combination", "departing_flight_id", "departing_price_rank"])
        sorted_df = utils.move_pandas_column_to_front(sorted_df, "leg")
        sorted_df = utils.move_pandas_column_to_front(sorted_df, "option")

        # export to CSV if requested
        if export_to:
            sorted_df.to_csv(export_to, index=False)

        return sorted_df


class CrazyLayoverItinerary(BaseItinerary):
    def __init__(
        self,
        search_query,
        direct_only,
        max_stops: int = 2,
        min_layover_time: timedelta = timedelta(hours=2),
        max_layover_time: timedelta = timedelta(hours=6),
    ):
        # TODO: maybe have n_layovers as a parameter in the SearchQuery object?
        assert max_stops in [2, 3], "Number of max stops must be 2 (one layover) or 3 (two layovers)."
        assert search_query.return_dates is None, "CrazyLayoverItinerary only supports one-way flights."
        assert max_layover_time > min_layover_time, "Max layover time must be greater than min layover time."
        assert max_layover_time < timedelta(hours=24), "Max layover time must be less than 24 hours."

        self.max_stops = max_stops
        self.min_layover_time = min_layover_time
        self.max_layover_time = max_layover_time  # TODO: use this parameter

        super().__init__(search_query, direct_only)

    def create_search_queries_from_connections(
        self,
        departure_aiports: List[Airport],
        arrival_airports: List[Airport],
        departure_dates=List[date],
        max_stops: int = 2,
    ) -> list[SingleItemSearchQuery]:
        """
        Create a list of SingleItemSearchQueries from all possible connections.

        :param departure_aiports: List of Airport objects for the departure airports.
        :param arrival_airports: List of Airport objects for the arrival airports.
        :param departure_dates: List of departure dates.
        :param max_stops: Maximum number of stops allowed in the connection.

        :return: List of SingleItemSearchQueries.
        """
        # get all possible connections between the departure and arrival airports
        connections = []
        for dep_airport in departure_aiports:
            for arr_airport in arrival_airports:
                connections += dep_airport.get_all_connections(arr_airport.iata, max_stops)

        search_queries = []

        # for each possible connection
        for connection in connections:
            # for each possible departure date
            for dep_date in departure_dates:
                # for each airport in the connection
                for airport_idx in range(len(connection)):
                    if airport_idx == len(connection) - 1:
                        break

                    sq = SingleItemSearchQuery(
                        airport_dep=Airport(connection[airport_idx]), airport_arr=Airport(connection[airport_idx + 1]), departure_date=dep_date
                    )
                    search_queries.append(sq)

        return search_queries

    def create_one_way_itineraries_from_search_queries(self, search_queries: List[SingleItemSearchQuery]) -> List[OneWayItinerary]:
        """
        Create a list of OneWayItinerary objects from a list of search queries.

        :param search_queries: List of search queries.

        :return: List of OneWayItinerary objects.
        """
        return [OneWayItinerary(search_query, direct_only=True) for search_query in search_queries]

    def run_scraper(self, itinerary: OneWayItinerary) -> pd.DataFrame:
        # print(f"Scraping connection {itinerary.search_query}")
        itinerary.scrape()
        return itinerary.df.copy()

    def create_search_queries_from_first_scrape_df(
        self, first_df: pd.DataFrame, arrival_airports: List[Airport], min_layover_time: timedelta, max_layover_time: timedelta
    ) -> List[SingleItemSearchQuery]:
        """
        :param df: DataFrame with the flights information from the first scrape.
        :param min_layover_time: Minimum layover time.
        :param max_layover_time: Maximum layover time.

        :return: List of SingleItemSearchQuery.
        """
        df = first_df.copy()

        df.datetime_dep = pd.to_datetime(df.datetime_dep)
        df.datetime_arr = pd.to_datetime(df.datetime_arr)

        # get leg 2 or more
        df = df.loc[df.airport_arr.isin([a.iata for a in arrival_airports])]

        df["possible_layover_up_to"] = df.datetime_arr + min_layover_time + max_layover_time
        df["possible_layover_up_to"] = df["possible_layover_up_to"].dt.strftime("%Y-%m-%d")

        df = df.drop_duplicates(subset=["airport_dep", "airport_arr", "possible_layover_up_to"], keep="first")
        df = df.reset_index(drop=True)

        search_queries = []
        for _, row in df.iterrows():
            departure_datetime = datetime.strptime(row.possible_layover_up_to, "%Y-%m-%d")
            departure_date = date(departure_datetime.year, departure_datetime.month, departure_datetime.day)

            sq = SingleItemSearchQuery(
                airport_dep=Airport(row.airport_dep),
                airport_arr=Airport(row.airport_arr),
                departure_date=departure_date,
            )
            search_queries.append(sq)

        return search_queries

    def scrape(
        self,
        departure_aiprorts: List[Airport] = None,
        arrival_airports: List[Airport] = None,
        departure_dates: List[date] = None,
        max_stops: int = 2,
        min_layover_time: timedelta = None,
        max_layover_time: timedelta = None,
        num_processes: int = 20,
        export_to: str = None,
    ):
        """
        Scrape the flights for all the possible connections using multiprocessing.
        After a first scrape, it will scrape again for flights that could have a layover in the following day due to the max_layover_time parameter.
        """
        if departure_aiprorts is None:
            departure_aiprorts = self.search_query.airports_dep
        if arrival_airports is None:
            arrival_airports = self.search_query.airports_arr
        if departure_dates is None:
            departure_dates = self.search_query.departure_dates
        if max_stops is None:
            max_stops = self.max_stops
        if min_layover_time is None:
            min_layover_time = self.min_layover_time
        if max_layover_time is None:
            max_layover_time = self.max_layover_time

        # first scrape
        first_search_queries = self.create_search_queries_from_connections(departure_aiprorts, arrival_airports, departure_dates, max_stops)
        print("FIRST SEARCH QUERIES: ", len(first_search_queries))
        first_scrape_df = self.do_scrape(first_search_queries, num_processes, debug_name="first")

        # second scrape
        second_search_queries = self.create_search_queries_from_first_scrape_df(first_scrape_df, arrival_airports, min_layover_time, max_layover_time)
        print("SECOND SEARCH QUERIES: ", len(second_search_queries))
        second_scrape_df = self.do_scrape(second_search_queries, num_processes, debug_name="second")

        full_df = pd.concat([first_scrape_df, second_scrape_df])

        # TODO: process the full_df to remove unwanted flights/impossible connections and
        # to group the possible options

        if export_to:
            full_df.to_csv(export_to, index=True)

    def do_scrape(self, search_queries: List[SingleItemSearchQuery], num_processes: int, debug_name: str = None) -> pd.DataFrame:
        one_way_itineraries = self.create_one_way_itineraries_from_search_queries(search_queries)

        manager = multiprocessing.Manager()
        all_flights_multiprocess = manager.list()

        def collect_result(result):
            all_flights_multiprocess.append(result)

        with multiprocessing.Pool(processes=num_processes) as pool:
            results = [pool.apply_async(self.run_scraper, args=(itinerary_dict,), callback=collect_result) for itinerary_dict in one_way_itineraries]
            pool.close()
            pool.join()
            pool.terminate()

        print("All flights scraped. Length: ", len(all_flights_multiprocess))

        flight_df = pd.concat(all_flights_multiprocess)
        if debug_name:
            flight_df.to_csv(f"flights_df_{debug_name}.csv", index=False)

        # return self.make_itinerary_df(flight_df)
        return flight_df

    def make_itinerary_df(self, flights_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a DataFrame with the flights information.
        """
        df = flights_df.copy().reset_index(drop=True)

        if df.empty:
            return pd.DataFrame()

        arrival_airports = [a.iata for a in self.search_query.airports_arr]
        return self._clean_df(df, arrival_airports)

    def _clean_df(self, df: pd.DataFrame, arrival_airports: list[str]) -> pd.DataFrame:
        # Initialize a list to store the indices of rows to remove
        indices_to_remove = []

        # Initialize dictionaries to store the minimum total price and minimum layover time for each option
        min_total_prices = {}
        min_layover_times = {}

        # First loop: Identify indices to remove and calculate minimum layover time
        for option, data in df.groupby("option"):
            # Determine the number of unique connection legs in the current option
            num_legs = data["connection_leg"].nunique()

            # Case 1: Single leg flight checks
            if num_legs == 1:
                # Check if it does not arrive in the arrival airports desired or is marked as the second leg
                if data["airport_arr"].mode().values[0] not in arrival_airports or data["connection_leg"].min() == 2:
                    indices_to_remove.extend(data.index.to_list())
                else:
                    # Single leg flight to FMM, consider its price as the total price and set layover time to 0
                    min_total_prices[option] = data["price"].min()
                    min_layover_times[option] = timedelta(0)

            # Case 2: Multiple leg flight checks
            elif num_legs > 1:
                # Separate the first and second leg flights
                first_leg_flights = data[data["connection_leg"] == 1]
                second_leg_flights = data[data["connection_leg"] == 2]

                # Determine the earliest possible departure time for the second leg after the minimum layover
                earliest_second_leg_time = pd.to_datetime(second_leg_flights["datetime_dep"].min()) - self.min_layover_time

                # Identify first leg flights that arrive too late for the layover
                late_first_leg_flights = first_leg_flights[pd.to_datetime(first_leg_flights["datetime_arr"]) > earliest_second_leg_time]

                # Add these late flights to the removal list
                indices_to_remove.extend(late_first_leg_flights.index)

                # If all first leg flights are too late, remove the entire option
                if len(late_first_leg_flights) == len(first_leg_flights):
                    indices_to_remove.extend(data.index)
                else:
                    # Find the minimum price combination of leg 1 and leg 2
                    min_first_leg_price = first_leg_flights[~first_leg_flights.index.isin(late_first_leg_flights.index)]["price"].min()
                    min_second_leg_price = second_leg_flights["price"].min()
                    min_total_prices[option] = min_first_leg_price + min_second_leg_price

                    # Calculate the minimum layover time
                    latest_first_leg_arrival = pd.to_datetime(
                        first_leg_flights[~first_leg_flights.index.isin(late_first_leg_flights.index)]["datetime_arr"]
                    ).max()
                    earliest_second_leg_departure = pd.to_datetime(second_leg_flights["datetime_dep"]).min()
                    layover_time = earliest_second_leg_departure - latest_first_leg_arrival
                    min_layover_times[option] = layover_time

        # Filter the DataFrame to exclude the identified rows
        filtered_df = df[~df.index.isin(indices_to_remove)].copy()

        # Map the minimum total prices and minimum layover times to the filtered DataFrame
        filtered_df["min_total_price"] = filtered_df["option"].map(min_total_prices)
        filtered_df["min_layover_time"] = filtered_df["option"].map(min_layover_times)

        return filtered_df
