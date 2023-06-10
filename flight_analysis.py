# author: Emanuele Salonico, 2023

import configparser
import json
import logging
from datetime import timedelta, datetime
import pandas as pd

from src.google_flight_analysis.scrape import Scrape
from src.google_flight_analysis.database import Database

import private.private as private

config = configparser.ConfigParser()
config.read('config.ini')

# TODO: improve
logger = logging.getLogger("flight_analysis")
logger.setLevel('DEBUG')
log_format = '%(asctime)s - %(levelname)s - %(message)s'
file_handler = logging.FileHandler("logs.log")
stream_handler = logging.StreamHandler()
formatter = logging.Formatter(log_format)
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)



def get_routes():
    """
    Returns a list of routes from the config file.
    """
    routes = []
    for route in config["routes"]:
        routes.append(json.loads(config["routes"][route]))
    
    return routes

if __name__ == "__main__":
    
    # 1. scrape routes
    routes = get_routes()
    all_results = []
    
    for route in routes:
        origin = route[0]
        destination = route[1]
        date_range = [datetime.today() + timedelta(days=i+1) for i in range(route[2])]
        date_range = [date.strftime("%Y-%m-%d") for date in date_range]

        for i, date in enumerate(date_range):
            scrape = Scrape(origin, destination, date)
            
            try:
                scrape.run_scrape()
                logger.info(f"[{i+1}/{len(date_range)}] Scraped: {origin} {destination} {date} - ({scrape.data.shape[0]}) results")
                all_results.append(scrape.data)
            except Exception as e:
                logger.error(f"[{i+1}/{len(date_range)}] ERROR WITH {origin} {destination} {date}")
                logger.error(e)

    all_results_df = pd.concat(all_results)
    
    logging.debug(all_results_df.head())
    
    # 2. add results to mongo_db
    database = Database(private.DB_URL, private.DB_NAME, private.DB_COLL)
    database.add_pandas_df(all_results_df)
    