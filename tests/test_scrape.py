import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from src.flight_analysis.scrape import Scrape
from src.flight_analysis.database import Database

import private.private as private
import configparser


def test_database_connection():
    db = Database(
        db_host=private.DB_HOST,
        db_name=private.DB_NAME,
        db_user=private.DB_USER,
        db_pw=private.DB_PW,
        db_table=private.DB_TABLE,
    )
    try:
        conn = db.connect_to_postgresql()
    except ConnectionError as e:
        assert False, e


# def test_dataset_generation():
#     ten_days_ahead = (datetime.today() + timedelta(5)).strftime("%Y-%m-%d")
#     scrape_obj = Scrape("MUC", "FCO", ten_days_ahead)
#     scrape_obj.run_scrape()
#     assert isinstance(scrape_obj.data, pd.DataFrame)


def test_config_file():
    try:
        config = configparser.ConfigParser()
        config.read("routes.ini")
    except Exception as e:
        assert False, e
