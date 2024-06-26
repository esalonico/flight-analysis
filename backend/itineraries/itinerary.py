import pandas as pd
from tqdm import tqdm

from backend.objects.airport import Airport
from backend.scrapers.one_way_scraper import OneWayScraper
from backend.scrapers.round_trip_scraper import RoundTripScraper
from backend.scrapers.search_query import SearchQuery, SingleItemSearchQuery
from backend.utils import utils


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
        for combination in tqdm(self.search_query.combinations):
            scraper = OneWayScraper(combination, self.direct_only)
            scraper.scrape()
            df = scraper.make_flights_df()
            combination_flights.append(df)

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
        for combination in tqdm(self.search_query.combinations):
            scraper = RoundTripScraper(combination, self.direct_only)
            scraper.scrape()
            df = scraper.make_flights_df()
            combination_flights.append(df)

        flight_df = pd.concat(combination_flights)
        self.df = self.make_itinerary_df(flight_df, export_to)

    def make_itinerary_df(self, flights_df: pd.DataFrame, export_to: str = None) -> pd.DataFrame:
        """
        Create a DataFrame with the flights information.

        :param flights_df: DataFrame with the flights information.
        :param export_to: Optional string with the filename to export the DataFrame to a CSV file.

        :return: DataFrame with the flights information.
        """
        df_in = flights_df.copy()

        # compute column for the flight leg (departing or returning)
        df_in["leg"] = df_in.apply(lambda x: self._compute_flight_leg_within_itinerary(self.search_query, x), axis=1)

        # assign each departing flight an ID of itself
        df_in["departing_flight_id"] = df_in["departing_flight_id"].fillna(df_in["_id"])
        df_in.to_csv("df_in.csv", index=False)

        # Compute column for the price rank within the departing flights to order them
        df_departing = df_in[df_in["leg"] == "departing"]
        df_departing.to_csv("departing.csv", index=False)
        df_departing["departing_price_rank"] = df_departing.groupby("departing_flight_id")["price"].rank(method="dense")

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

    def _compute_flight_leg_within_itinerary(self, sq: SearchQuery, flight: pd.Series) -> str:
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


class CrazyLayoverItinerary(BaseItinerary):
    def __init__(self, search_query, direct_only, max_stops: int = 2):
        # TODO: maybe have n_layovers as a parameter in the SearchQuery object?
        assert max_stops in [2, 3], "Number of max stops must be 2 (one layover) or 3 (two layovers)."
        assert search_query.return_dates is None, "CrazyLayoverItinerary only supports one-way flights."
        self.max_stops = max_stops

        super().__init__(search_query, direct_only)

    def create_search_queries_from_connections(self) -> list[dict]:
        """
        Create a list of Single Item Search Queries from all possible connections.
        """
        connections = []
        for dep_airport in self.search_query.airports_dep:
            for arr_airport in self.search_query.airports_arr:
                connections += dep_airport.get_all_connections(arr_airport.iata, self.max_stops)

        search_queries = []
        connection_id = 0

        # for each possible connection
        for connection in connections:

            # for each possible departure date
            for dep_date in self.search_query.departure_dates:

                # for each airport in the connection
                for airport_idx in range(len(connection)):
                    if airport_idx == len(connection) - 1:
                        break

                    sq = SingleItemSearchQuery(
                        airport_dep=Airport(connection[airport_idx]), airport_arr=Airport(connection[airport_idx + 1]), departure_date=dep_date
                    )
                    data = {"connection_id": connection_id, "connection_leg": airport_idx + 1, "search_query": sq}
                    search_queries.append(data)

                connection_id += 1

        return search_queries

    def create_one_way_itineraries_from_search_queries(self, search_queries: list[dict]) -> list[OneWayItinerary]:
        """
        Create a list of OneWayItinerary objects from a list of search queries.

        :param search_queries: List of search queries.

        :return: List of OneWayItinerary objects.

        Example of return:
        [
            {
                "itinerary": OneWayItinerary,
                "connection_id": 1,
                "connection_leg": 1,
                "search_query": SingleItemSearchQuery(FCO, FMM, 2024-07-25)
            },
            ...
        ]
        """
        one_way_itineraries = []
        for search_query in search_queries:
            itinerary = OneWayItinerary(search_query["search_query"], direct_only=True)
            data = {"itinerary": itinerary} | search_query
            one_way_itineraries.append(data)

        return one_way_itineraries

    def scrape(self):
        search_queries = self.create_search_queries_from_connections()
        one_way_itineraries = self.create_one_way_itineraries_from_search_queries(search_queries)

        one_way_itineraries_dfs = []
        for itinerary in tqdm(one_way_itineraries):
            try:
                itinerary["itinerary"].scrape()
                df = itinerary["itinerary"].df
                df["option"] = itinerary["connection_id"]
                df["connection_leg"] = itinerary["connection_leg"]
                one_way_itineraries_dfs.append(df)
            except Exception as e:
                print(f"Error in connection {itinerary['connection_id']}: {e}")

            # if itinerary["connection_id"] >= 20:
            #     break

        flight_df = pd.concat(one_way_itineraries_dfs)
        flight_df.to_csv("crazy_layover.csv", index=False)

    def make_itinerary_df(self, flights_df: pd.DataFrame, export_to: str = None) -> pd.DataFrame:
        df = flights_df.copy()

        if df.empty:
            return pd.DataFrame()

        # TODO: implement this method
