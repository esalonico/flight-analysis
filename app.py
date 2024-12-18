import ast
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

import src.flights.utils.utils as utils
from src.flights.models.models import Airport, CompositeSearch
from src.flights.scrapers.scrapers import OneWayScraper

MAX_INPUT_AIRPORTS = 5

# configure streamlit
st.set_page_config(layout="wide")
if "search" not in st.session_state:
    st.session_state.search = CompositeSearch()


# FUNCTIONS
def build_search_object():
    st.session_state.search.origins = [Airport(iata.split()[0]) for iata in origins]
    st.session_state.search.destinations = [Airport(iata.split()[0]) for iata in destinations]
    st.session_state.search.departure_dates = utils.generate_date_range_between_dates(*departure_dates)
    st.session_state.search.direct_only = direct_only


def render_df(df):
    def format_flight_time(input_mins: Optional[int]) -> Optional[str]:
        """
        Format flight time in minutes to human-readable format.
        :param input_mins: Flight time in minutes.

        :return: Human-readable flight time e.g. "2 hr 30 min". Returns None if input is None.
        """
        if not input_mins or np.isnan(input_mins):
            return None

        hours = int(input_mins // 60)
        mins = int(input_mins % 60)

        if hours > 0:
            return f"{hours} hr {mins} min" if mins > 0 else f"{hours} hr"
        else:
            return f"{mins} min"

    df["flight_time"] = df["flight_time"].apply(format_flight_time)
    df["layover_time"] = df["layover_time"].apply(format_flight_time)

    st.dataframe(
        df,
        column_config={
            "dep_datetime": st.column_config.DatetimeColumn(format="D MMM, HH:mm"),
            "arr_datetime": st.column_config.DatetimeColumn(format="D MMM, HH:mm"),
            "airline_logo_url": st.column_config.ImageColumn(label="", width=None),
            "price": st.column_config.ProgressColumn(format="€%f", min_value=0, max_value=int(df.price.max())),
        },
        height=600 if len(df) > 10 else None,
    )


def run():
    build_search_object()

    scraper = OneWayScraper(st.session_state.search)
    all_flights = scraper.scrape_all_flights()

    if not all_flights:
        st.warning("No flights found for this single search.")
        return

    df = scraper.make_flights_dataframe(all_flights)
    render_df(df)


# INPUTS
# TODO: delete
def read_flights_csv(filepath: str = "debug/flights.csv") -> pd.DataFrame:
    df = pd.read_csv(filepath)
    df["airlines"] = df["airlines"].apply(ast.literal_eval)
    df["layover_location"] = df["layover_location"].apply(ast.literal_eval)
    return df


st.button("Render DF", on_click=lambda: render_df(read_flights_csv()))


origins = st.multiselect(
    "Origin airport(s)",
    options=Airport.get_all_iatas_with_names(),
    # default=["FCO (Rome\u2013Fiumicino Leonardo da Vinci International Airport)"],
    default=["MIA (Miami International Airport)"],
    max_selections=MAX_INPUT_AIRPORTS,
)
destinations = st.multiselect(
    "Destination airport(s)",
    options=Airport.get_all_iatas_with_names(),
    # default=["MUC (Munich Airport)"],
    default=["LAX (Los Angeles International Airport)"],
    max_selections=MAX_INPUT_AIRPORTS,
)

departure_dates = st.date_input(
    "Departure date(s)",
    value=(
        datetime.today() + timedelta(days=10),
        datetime.today() + timedelta(days=12),
    ),
    min_value=datetime.today(),
    format="DD-MM-YYYY",
)
direct_only = st.checkbox("Direct flights only", value=False)

# apply button
st.button("Apply", on_click=lambda: run())
