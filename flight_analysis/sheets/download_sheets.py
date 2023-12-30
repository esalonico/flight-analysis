"""
Downloads sheets needed for the scraping process if they are not there already.
"""
import os

import pandas as pd


def _download_airports_sheet() -> None:
    """
    Downloads the airports.csv file from datahub.io if it is not there already.
    """
    url = "https://datahub.io/core/airport-codes/r/airport-codes.csv"
    filename = "airports.csv"
    filepath = os.path.join(os.path.dirname(__file__), filename)
    
    # if file already exists, do nothing
    if os.path.isfile(filepath):
        return
    
    df = pd.read_csv(url)
    
    # get only airports that have a IATA code
    df = df[(df["iata_code"].notnull()) & (df.iata_code.str.len() == 3)]

    # get only useful columns
    df = df[["iata_code", "name", "iso_country", "iso_region", "municipality", "type", "coordinates"]]

    # set iata_code as index
    df = df.set_index("iata_code").sort_index()

    # split coordinates into lat and long
    df[["lat", "lon"]] = df["coordinates"].str.split(",", expand=True).astype(float).round(4)
    df = df.drop("coordinates", axis=1)

    # clean up "type" column
    df["type"] = df["type"].str.replace("_airport", "")
    
    # fix encoding of airport name
    df["name"] = df["name"].str.encode('latin1').str.decode('utf-8')
    
    # remove duplicates
    df = df[~df.index.duplicated(keep="first")]
    
    # save file to csv
    df.to_csv(filepath, encoding='utf-8')


def _download_countries_sheet() -> None:
    """
    Downloads the countries.csv file from github if it is not there already.
    """
    url = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
    filename = "countries.csv"
    filepath = os.path.join(os.path.dirname(__file__), filename)
    
    # if file already exists, do nothing
    if os.path.isfile(filepath):
        return
    
    df = pd.read_csv(url)
    
    # rename columns: change - to _
    df.columns = df.columns.str.replace('-', '_')

    # set correct index
    df = df.set_index('alpha_2')

    # take only needed columns
    df = df[['name', 'region', 'sub_region']]
    
    # save file to csv
    df.to_csv(filepath)
    

def download_all_sheets() -> None:
    """
    Downloads all sheets.
    """
    _download_airports_sheet()
    _download_countries_sheet()