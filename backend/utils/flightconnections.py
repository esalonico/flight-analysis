# https://www.flightconnections.com/airports_url.php?lang=en&iata=FCO
#  { "a": "Rome (Fiumicino) (FCO)", "c": 262 }


# https://www.flightconnections.com/ro357.json?v=1097&
# lang=en
# &f=no0
# &direction=from
# &exc=
# &ids=
# &cl=
# &flight_direction=from
# &flight_type=round
# &airlines=
# &alliance=
# &classes=
# &dates=
# &dates_type=
# &days_in_destination=
# &aircrafts=


"""
Step 1: iterate over all IATA codes and get their flightconnection.com ID
# https://www.flightconnections.com/airports_url.php?lang=en&iata={ITERATE_IATA_HERE}
# Save them to a csv file in the data folder

Step 2: iterate over all IATA codes and get their flight connections
....
"""

from json.decoder import JSONDecodeError

import pandas as pd
import requests
from tqdm import tqdm

API_VERSION = 1097


def get_airport_id_from_api(iata_code: str) -> dict:
    """
    Get the airport name and ID from the flightconnections.com API

    :param iata_code: IATA code of the airport
    :return: dict with the airport name and ID

    Example:
    get_airport_id_from_api("FCO") -> {"name": "Rome (Fiumicino) (FCO)", "code": 262}
    """
    url = f"https://www.flightconnections.com/airports_url.php?lang=en&iata={iata_code}"
    response = requests.get(url)
    try:
        data = response.json()
    except JSONDecodeError:
        data = {}

    return {"name": data.get("a"), "code": data.get("c")}


def create_airports_codes_df(iata_airports_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame with the airport name and ID from the flightconnections.com API

    :param iata_airports_df: DataFrame with the IATA codes of the airports
    :return: DataFrame with the airport name and ID
    """
    all_airports = []
    for i, row in tqdm(iata_airports_df.iterrows(), total=len(iata_airports_df)):
        data = {"iata_code": row["iata_code"]} | get_airport_id_from_api(row["iata_code"])
        if data.get("code") is not None:
            all_airports.append(data)

    return pd.DataFrame(all_airports)


def get_airport_connection_from_code(flightconnection_code: int):
    url = f"https://www.flightconnections.com/ro{flightconnection_code}.json?v={API_VERSION}&lang=en&f=no0&direction=from"
    response = requests.get(url)
    try:
        data = response.json()
    except JSONDecodeError:
        data = {}
        data["all"] = []
    return data["all"]


def create_airport_connections_df(airports_flightconnections_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a DataFrame with the airport connections from the flightconnections.com API

    :param airports_df: DataFrame with the airport name and ID
    :return: DataFrame with the airport connections
    """
    all_connections = []
    for _, row in tqdm(airports_flightconnections_df.iterrows(), total=len(airports_flightconnections_df)):
        data = get_airport_connection_from_code(row["code"])
        connection_data = {"iata_code": row["iata_code"], "connections": data}
        all_connections.append(connection_data)

    df = pd.DataFrame(all_connections)

    # explode the connections column
    df = df.explode("connections").reset_index(drop=True)

    # replace the connections column with the IATA values from airports_flightconnections_df
    df["connections"] = df["connections"].replace(airports_flightconnections_df.set_index("code")["iata_code"])

    # rename the columns
    df = df.rename(columns={"iata_code": "iata_code_from", "connections": "iata_code_to"})

    # remove rows where iata_code_from is equal to iata_code_to (self connections)
    df = df[df["iata_code_from"] != df["iata_code_to"]]

    # drop na values
    df = df[df["iata_code_to"].notna()]

    # remove rows whose iata_code_to doesn't exist in the airports_flightconnections_df
    pattern = r"^[A-Z]{3}$"
    df = df[df["iata_code_to"].str.match(pattern, na=False)]

    return df.reset_index(drop=True)


if __name__ == "__main__":
    # iata_df = pd.read_csv("backend/data/sheets/airports.csv")
    # flightconnections_df = create_flightconnections_airports_codes_df(iata_df)
    # flightconnections_df.to_csv("backend/data/sheets/airports_flightconnections.csv", index=False)

    # connections = get_airport_connection_from_code(262)
    # print(connections)

    df = create_airport_connections_df(pd.read_csv("backend/data/sheets/airports_flightconnections.csv"))
    df.to_csv("backend/data/sheets/connections_flightconnections.csv", index=False)
