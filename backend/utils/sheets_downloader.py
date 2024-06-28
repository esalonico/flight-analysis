"""
Downloads sheets needed for the scraping process if they are not there already.
"""

import logging
import os

import pandas as pd

import backend.utils.flightconnections as flightconnections
from backend.utils.utils import setup_logging

setup_logging()
logger = logging.getLogger(os.path.basename(__file__))


def _download_airports_sheet(force_download: bool = False) -> None:
    """
    Downloads the airports.csv file from datahub.io if it is not there already.

    :param force_download: if True, the file will be downloaded even if it already exists
    """
    url = "https://davidmegginson.github.io/ourairports-data/airports.csv"
    filename = "airports.csv"
    filepath = f"backend/data/sheets/{filename}"

    # if file already exists, do nothing unless force_download is True
    if os.path.isfile(filepath) and not force_download:
        logger.debug(f"File already exists: {filepath}")
        return

    df = pd.read_csv(url)

    # get only airports that have a IATA code
    df = df[(df["iata_code"].notnull()) & (df.iata_code.str.len() == 3)]

    # get only useful columns
    df = df[["iata_code", "name", "iso_country", "iso_region", "municipality", "type", "latitude_deg", "longitude_deg"]]

    # set iata_code as index
    df = df.set_index("iata_code").sort_index()

    # rename columns
    df = df.rename(columns={"latitude_deg": "lat", "longitude_deg": "lon"})

    # clean up "type" column
    df["type"] = df["type"].str.replace("_airport", "")

    # remove duplicates
    df = df[~df.index.duplicated(keep="first")]

    # save file to csv
    df.to_csv(filepath, encoding="utf-8")
    logger.info(f"Downloaded file: {filepath}")


def _download_countries_sheet(force_download: bool = False) -> None:
    """
    Downloads the countries.csv file from github if it is not there already.

    :param force_download: if True, the file will be downloaded even if it already exists
    """
    url = "https://raw.githubusercontent.com/lukes/ISO-3166-Countries-with-Regional-Codes/master/all/all.csv"
    filename = "countries.csv"
    filepath = f"backend/data/sheets/{filename}"

    # if file already exists, do nothing unless force_download is True
    if os.path.isfile(filepath) and not force_download:
        logger.debug(f"File already exists: {filepath}")
        return

    df = pd.read_csv(url)

    # rename columns: change - to _
    df.columns = df.columns.str.replace("-", "_")

    # set correct index
    df = df.set_index("alpha_2")

    # take only needed columns
    df = df[["name", "region", "sub_region"]]

    # save file to csv
    df.to_csv(filepath)
    logger.info(f"Downloaded file: {filepath}")


def _download_flightconnections_airport_codes_sheet(force_download: bool = False) -> None:
    filepath = "backend/data/sheets/airports_flightconnections.csv"

    # if file already exists, do nothing unless force_download is True
    if os.path.isfile(filepath) and not force_download:
        logger.debug(f"File already exists: {filepath}")
        return

    iata_airports_filepath = "backend/data/sheets/airports.csv"
    assert os.path.isfile(iata_airports_filepath), "airports.csv file not found. Please download it first."
    iata_df = pd.read_csv(iata_airports_filepath)

    # scrape
    df = flightconnections.create_airports_codes_df(iata_df)

    # export to csv
    df.to_csv(filepath, index=False)
    logger.info(f"Downloaded file: {filepath}")


def _download_flightconnections_airport_connections_sheet(force_download: bool = False) -> None:
    filepath = "backend/data/sheets/connections_flightconnections.csv"

    # if file already exists, do nothing unless force_download is True
    if os.path.isfile(filepath) and not force_download:
        logger.debug(f"File already exists: {filepath}")
        return

    flightconnections_codes_filepath = "backend/data/sheets/airports_flightconnections.csv"
    assert os.path.isfile(flightconnections_codes_filepath), "airports_flightconnections.csv file not found. Please download it first."
    flightconnections_codes_df = pd.read_csv(flightconnections_codes_filepath)

    # scrape
    df = flightconnections.create_airport_connections_df(flightconnections_codes_df)

    # export to csv
    df.to_csv(filepath, index=False)
    logger.info(f"Downloaded file: {filepath}")


def download_all_sheets(force_download: bool = False):
    """
    Downloads all sheets.

    :param force_download: if True, the files will be downloaded even if they already exist
    """
    logger.debug("Downloading all sheets...")
    _download_airports_sheet(force_download)
    _download_countries_sheet(force_download)
    _download_flightconnections_airport_codes_sheet(force_download)
    _download_flightconnections_airport_connections_sheet(force_download)
