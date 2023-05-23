import pytest
import pandas as pd
from selenium import webdriver

from src.google_flight_analysis.scrape import *

def test_chromedriver_found():
    chrome_driver = webdriver.Chrome()
    assert chrome_driver is not None
    
def test_dataset_generation():
    scrape_obj = Scrape("MUC", "FCO", "2023-11-22")
    assert isinstance(scrape_obj.data, pd.DataFrame)
