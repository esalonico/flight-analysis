# author: Emanuele Salonico, 2023

import utils
import os
import sys
import uuid

# logging
logger_name = os.path.basename(__file__)
logger = utils.setup_logger(logger_name)

import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import configparser
import json

import private.private as private

from src.flight_analysis.scrape import Scrape
from src.flight_analysis.database import Database


def read_routes_from_file():
    """
    Returns a list of routes from the routes.ini file.
    """
    # TODO: test this function: test that the file routes.ini exists,
    # that it has the correct format and that the routes are not empty.
    # TODO: write some test cases in tests.py
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "routes.ini"))

    routes = []
    for route in config["routes"]:
        routes.append(json.loads(config["routes"][route]))

    return routes


def get_date_range_from_today_to_flight(n_days_advance: int):
    """
    Returns a list of dates from today to the flight date.
    """
    date_range = [
        (datetime.today() + timedelta(days=i + 1)) for i in range(n_days_advance)
    ]
    date_range = [date.strftime("%Y-%m-%d") for date in date_range]
    return date_range


def compute_iteration_time(start: datetime, end: datetime):
    """
    Returns the time in seconds between two datetime objects.
    """
    s = (end - start).seconds
    ms = round(((end - start).microseconds * 1e-6), 2)
    return s + ms


def get_routes_df(routes: list):
    """
    Returns a pandas dataframe with all the scraped results.

    Input: routes (list of lists)
    """
    # compute number of total scrapes
    n_total_scrapes = sum([route[2] for route in routes])

    all_results = []
    all_iter_times = []
    n_iter = 1

    # iterate over the routes
    for route in routes:
        origin = route[0]
        destination = route[1]
        n_days_advance = route[2]
        date_range = get_date_range_from_today_to_flight(n_days_advance)

        # iterate over dates
        for i, date in enumerate(date_range):
            scrape = Scrape(origin, destination, date)

            try:
                time_start = datetime.now()

                # run scrape
                scrape.run_scrape()

                time_end = datetime.now()
                time_iteration = compute_iteration_time(time_start, time_end)
                all_iter_times.append(time_iteration)
                avg_iter_time = round(np.array(all_iter_times).mean(), 2)
                logger.info(
                    f"[{n_iter}/{n_total_scrapes}] [{time_iteration} sec - avg: {avg_iter_time}] Scraped: {origin} {destination} {date} - {scrape._data.shape[0]} results"
                )

                # append scrape results to all_results
                all_results.append(scrape._data)

            except Exception as e:
                logger.error(f"ERROR: {origin} {destination} {date}", e)

            n_iter += 1

        # concatenate all results into a single dataframe
        final_df = pd.concat(all_results).reset_index(drop=True)

        # clean and transform the dataframe
        # convert layover time and travel time to minutes
        final_df["layover_time"] = final_df["layover_time"].dt.total_seconds() / 60
        final_df["layover_time"] = final_df["layover_time"].fillna(0).astype(int)
        final_df["travel_time"] = final_df["travel_time"].dt.total_seconds() / 60
        final_df["travel_time"] = final_df["travel_time"].fillna(0).astype(int)

        final_df["price_value"] = (
            final_df["price_value"].fillna(np.nan).replace([np.nan], [None])
        )

        final_df["uuid"] = [uuid.uuid4() for _ in range(final_df.shape[0])]
        final_df = final_df.set_index("uuid")

        final_df.to_csv(f"{route}.csv")

    return final_df


def generate_airlines_df_from_flights(flights_df):
    """
    From a flights dataframe, generate an airline dataframe.
    Goal: respect good database conditions.
    """
    # check if all indices are unique
    if not flights_df.index.is_unique:
        flights_df = flights_df.reset_index(drop=True)

    # create a dataframe with all the airlines, referencing the index
    airlines_df = flights_df.explode("airlines")[["airlines"]].reset_index()

    # rename column to "airline"
    airlines_df = airlines_df.rename(
        columns={"uuid": "flight_uuid", "airlines": "airline"}
    )

    # rename index to uuid
    airlines_df.index.names = ["uuid"]

    return airlines_df


def generate_layovers_df_from_flights(flights_df):
    """
    From a flights dataframe, generate a layovers dataframe.
    Goal: respect good database conditions.
    """
    # check if all indices are unique
    if not flights_df.index.is_unique:
        flights_df = flights_df.reset_index(drop=True)

    # create a dataframe with all the layovers, referencing the index
    layovers_df = flights_df.explode("layover_location")[
        ["layover_location"]
    ].reset_index()

    # get rid of rows with no layover
    layovers_df = layovers_df.dropna().reset_index(drop=True)

    # rename column
    layovers_df = layovers_df.rename(columns={"uuid": "flight_uuid"})

    # rename index to uuid
    layovers_df.index.names = ["uuid"]

    return layovers_df


if __name__ == "__main__":
    SKIP_SAVE_TO_DB = len(sys.argv) > 1 and sys.argv[1] == "nodb"

    # retreive routes to scrape
    routes = read_routes_from_file()

    # scrape routes into a dataframe
    scraped_flights = get_routes_df(routes)
    print(scraped_flights)

    # generate airline and layovers dataframe
    scraped_airlines = generate_airlines_df_from_flights(scraped_flights)
    scraped_layovers = generate_layovers_df_from_flights(scraped_flights)

    # drop airlines and layovers from flights dataframe
    scraped_flights = scraped_flights.drop(columns=["airlines", "layover_location"])

    # add results to database
    if not SKIP_SAVE_TO_DB:
        # connect to database
        db = Database(
            db_host=private.DB_HOST,
            db_name=private.DB_NAME,
            db_user=private.DB_USER,
            db_pw=private.DB_PW,
        )

        # prepare database and tables
        db.prepare_db_and_tables()

        # add dataframes to database
        db.add_pandas_df_to_db(scraped_flights, table_name=db.table_scraped)
        db.add_pandas_df_to_db(scraped_airlines, table_name=db.table_scraped_airlines)
        db.add_pandas_df_to_db(scraped_layovers, table_name=db.table_scraped_layovers)

    # if it's a monday, backup the database
    if datetime.today().weekday() == 0:
        # dump database to file
        db.dump_database_to_file()

        # handle database backup rotation
        db.rotate_database_backups()
