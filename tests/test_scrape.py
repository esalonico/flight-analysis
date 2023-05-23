import pytest
import pandas as pd
from selenium import webdriver
import numpy as np

from src.google_flight_analysis.scrape import *

test_df = pd.read_csv("./outputs/MUC_JFK_test.csv")

def test_dataframe_type():
    assert isinstance(test_df, pd.DataFrame)
    
def test_dataframe_shape():
    assert np.array_equal(test_df.shape, np.array([241, 16]))

# def test_chromedriver_found():
#     chrome_driver = webdriver.Chrome(executable_path="/usr/bin/google-chrome")
#     assert chrome_driver is not None
    
# def test_dataset_generation():
#     scrape_obj = Scrape("MUC", "FCO", "2023-11-22")
#     assert isinstance(scrape_obj.data, pd.DataFrame)
