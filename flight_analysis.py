# author: Emanuele Salonico, 2023

import utils
import os
import sys

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

    return pd.concat(all_results)


if __name__ == "__main__":
    SKIP_SAVE_TO_DB = len(sys.argv) > 1 and sys.argv[1] == "nodb"

    # retreive routes to scrape
    routes = read_routes_from_file()

    # scrape routes into a dataframe
    scraped_flights = get_routes_df(routes)

    # connect to database
    db = Database(
        db_host=private.DB_HOST,
        db_name=private.DB_NAME,
        db_user=private.DB_USER,
        db_pw=private.DB_PW,
        db_table=private.DB_TABLE,
    )

    # prepare database and tables
    db.prepare_db_and_tables(overwrite_table=False)

    # add results to database
    if not SKIP_SAVE_TO_DB:
        db.add_pandas_df_to_db(scraped_flights)

    # handle backup here
    db.dump_database_to_sql_file()
