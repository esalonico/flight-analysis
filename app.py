from datetime import datetime, timedelta

import streamlit as st

import src.flights.utils.utils as utils
from src.flights.models.models import Airport, SearchParameters

MAX_INPUT_AIRPORTS = 5

# configure streamlit
st.set_page_config(layout="wide")
if "search_parameters" not in st.session_state:
    st.session_state.search_parameters = SearchParameters()


# FUNCTIONS
def build_search_object():
    st.session_state.search_parameters.origins = [
        Airport(iata.split()[0]) for iata in origins
    ]
    st.session_state.search_parameters.destinations = [
        Airport(iata.split()[0]) for iata in destinations
    ]
    st.session_state.search_parameters.departure_dates = (
        utils.generate_date_range_between_dates(*departure_dates)
    )
    st.session_state.search_parameters.direct_only = direct_only


# INPUTS
origins = st.multiselect(
    "Origin airport(s)",
    options=Airport.get_all_iatas_with_names(),
    default=["FCO (Rome\u2013Fiumicino Leonardo da Vinci International Airport)"],
    max_selections=MAX_INPUT_AIRPORTS,
)
destinations = st.multiselect(
    "Destination airport(s)",
    options=Airport.get_all_iatas_with_names(),
    default=["MUC (Munich Airport)"],
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
direct_only = st.checkbox("Direct flights only", value=True)

# apply button
st.button("Apply", on_click=lambda: build_search_object())

# sidebar
with st.sidebar:
    st.write("Sidebar")


# debug
st.write(st.session_state)
