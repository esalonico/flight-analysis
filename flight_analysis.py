# author: Emanuele Salonico, 2023

import utils
import os

# logging
logger_name = os.path.basename(__file__)
logger = utils.setup_logger(logger_name)

import numpy as np
import pandas as pd
from datetime import timedelta, datetime
import configparser

from src.google_flight_analysis.scrape import Scrape
from src.google_flight_analysis.database import Database
import private.private as private

# config
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "routes.ini"))


if __name__ == "__main__":

    # 1. scrape routes
    routes = utils.get_routes_from_config(config)
    
    # compute number of total scrapes
    n_total_scrapes = sum([x[2] for x in routes])
    
    all_results = []
    all_iter_times = []
    n_iter = 1

    # iterate over the routes
    for route in routes:
        origin = route[0]
        destination = route[1]
        date_range = [(datetime.today() + timedelta(days=i+1)) for i in range(route[2])]
        date_range = [date.strftime("%Y-%m-%d") for date in date_range]

        # iterate over dates
        for i, date in enumerate(date_range):
            scrape = Scrape(origin, destination, date)

            try:
                time_start = datetime.now()
                
                # run scrape
                scrape.run_scrape()
                
                time_end = datetime.now()

                time_iteration = (time_end - time_start).seconds + round(((time_end - time_start).microseconds * 1e-6), 2)
                all_iter_times.append(time_iteration)
                avg_iter_time = round(np.array(all_iter_times).mean(), 2)

                logger.info(f"[{n_iter}/{n_total_scrapes}] [{time_iteration} sec - avg: {avg_iter_time}] Scraped: {origin} {destination} {date} - {scrape.data.shape[0]} results")
                all_results.append(scrape.data)
            except Exception as e:
                logger.error(f"ERROR: {origin} {destination} {date}")
                logger.error(e)
                
            n_iter += 1

    all_results_df = pd.concat(all_results)

    # 2. add results to postgresql
    # connect to database
    db = Database(db_host=private.DB_HOST, db_name=private.DB_NAME, db_user=private.DB_USER, db_pw=private.DB_PW, db_table=private.DB_TABLE)

    # prepare database and tables
    db.prepare_db_and_tables(overwrite_table=False)

    # add results to database
    db.add_pandas_df_to_db(all_results_df)
