# author: Emanuele Salonico, 2023

import configparser
import json
from datetime import timedelta, datetime
import pandas as pd

from src.google_flight_analysis.scrape import Scrape
from src.google_flight_analysis.database import Database

import private.private as private

config = configparser.ConfigParser()
config.read('config.ini')

def get_routes():
    """
    Returns a list of routes from the config file.
    """
    routes = []
    for route in config["routes"]:
        routes.append(json.loads(config["routes"][route]))
    
    return routes

if __name__ == "__main__":
    routes = get_routes()
    all_results = []
    
    for route in routes:
        origin = route[0]
        destination = route[1]
        date_range = [datetime.today() + timedelta(days=i+1) for i in range(route[2])]
        date_range = [date.strftime("%Y-%m-%d") for date in date_range]
        
        for date in date_range:
            scrape = Scrape(origin, destination, date)
            print(origin, destination, date, scrape.url)
            scrape.run_scrape()
            
            all_results.append(scrape.data)
            
    all_results_df = pd.concat(all_results)
    
    print(all_results_df.head())
    print(all_results_df.shape)
    
    # add results to mongo_db
    db = Database(private.DB_URL, private.DB_NAME, private.DB_COLL)
    
    db.add_pandas_df(all_results_df)