# custom written API code to retrieve flight connections data from flightconnections.com
import pandas as pd
import requests
from tqdm import tqdm

BASE_URL = "https://www.flightconnections.com/"
API_VERSION = 1073


def _get_airport_code_from_iata(iata_code):
    url = BASE_URL + f"airports_url.php?lang=en&iata={iata_code}"
    try:
        full_request = requests.get(url).json()
        return full_request["c"]
    except Exception as e:
        return None

def make_df_of_all_airport_codes(iata_codes):
    res = dict()
    for iata in tqdm(iata_codes):
        code = _get_airport_code_from_iata(iata)

        res[iata] = code

    df = pd.DataFrame.from_dict(res, orient="index").reset_index()
    df = df.rename(columns={"index": "iata", 0: "code"})

    # remove na
    df = df[df["code"].notna()]
    df.code = df.code.astype(int)

    df = df.sort_values(by="code")
    df = df.reset_index(drop=True)

    # save to csv
    df.to_csv("src/flight_analysis/apis/flightconnections_airport_codes.csv", index=False)

    return df

def _get_connections_from_airport_code(airport_code, direction="from", api_version=API_VERSION):
    url = BASE_URL + f"ro{airport_code}.json?v={api_version}&direction={direction}"
    try:
        full_request = requests.get(url).json()
        conn = [x for x in full_request["pts"] if x != airport_code]
    except Exception as e:
        conn = []
    
    return conn

def make_df_of_all_connections(airport_codes_df):
    df = airport_codes_df.copy()
    df["to"] = pd.NA

    for i, row in tqdm(df.iterrows(), total=df.shape[0]):
        airport_from = row.code
        airports_to = _get_connections_from_airport_code(airport_from, "from")
        df.at[i, "to"] = airports_to
        
    # explode the list of airports: each row will only be a from-to routes
    df2 = df.explode("to")
    
    # replace numeric airport codes with IATA codes (e.g. 1 -> AMS)
    df3 = df2.merge(airport_codes_df, left_on="to", right_on="code", how="left", suffixes=("_from", "_to"))[["iata_from", "iata_to"]]
    df3 = df3.loc[df3.iata_to.notna()] # TODO: fix some missing values
    
    # save to csv
    df3.to_csv("src/flight_analysis/apis/flightconnections_airport_connections.csv", index=False)
        
    return df3