from datetime import date, timedelta
from typing import List

import pandas as pd


def generate_date_range_between_dates(start_date: date, end_date: date) -> List[date]:
    """
    Generate a list of dates between two dates

    :param start_date: the start date
    :param end_date: the end date
    :return: a list of dates between the two dates
    """
    assert start_date <= end_date, "Start date must be before or equal to end date"

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)
    return date_list


def move_column_to_position(df: pd.DataFrame, col_name: str, position: int) -> pd.DataFrame:
    """
    Move a column to a specific position in a DataFrame.

    :param df: DataFrame
    :param col_name: Name of the column to move
    :param position: Position to move the column to
    :return: DataFrame with the column moved to the specified position
    """
    cols = df.columns.tolist()
    cols.remove(col_name)
    cols.insert(position, col_name)
    return df[cols]
