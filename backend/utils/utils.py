import logging.config
import os
import re
from datetime import date, datetime, timedelta

import pandas as pd


def is_date_in_future(input_date: date) -> bool:
    """
    Checks if a date is in the future (or today).

    :param input_date: Date object to check.
    :return: True if date is in the future (or today), False otherwise.
    """
    return input_date >= date.today()


def calculate_delta_days(date_from: datetime, date_to: datetime) -> int:
    """
    Calculates the number of days between two dates.
    Returns: Number of days as integer.
    """
    # if object is datetime, convert to date
    if isinstance(date_from, datetime):
        date_from = date_from.date()
    if isinstance(date_to, datetime):
        date_to = date_to.date()

    return (date_to - date_from).days


def convert_string_date_time_to_datetime(date_str: str, time_str: str) -> datetime:
    """
    Convert a string date and time to a datetime object.
    - date_str: Date string in the format of "YYYY-MM-DD"
    - time_str: Time string in the format of "HH:MM AM/PM"
    Also handles +- X days in the time_str (time zone difference)
    Returns: datetime object.
    """
    # handle + - days: extract the optional delta value (in case of +- X days)
    delta_days = 0
    if time_str[-2] == "+":
        delta_days = int(time_str[-1])
    elif time_str[-2] == "-":
        delta_days = -int(time_str[-1])

    delta = timedelta(days=delta_days)

    # remove the delta value from the argument if present
    if delta_days != 0:
        time_str = time_str[:-2]  # from 10:30PM+1 to 10:30PM

    # combine date and time into one string (using f string)
    date_time_str = f"{date_str} {time_str}"

    # convert to datetime object
    datetime_obj = datetime.strptime(date_time_str, "%Y-%m-%d %I:%M%p") + delta

    return datetime_obj


def convert_string_to_duration(duration_str: str) -> timedelta:
    """
    Convert a string duration to a timedelta object.
    - duration_str: Duration string in the format of "X hr X min"
    Returns: timedelta object.
    """
    # case 1: "X hr X min"
    if "hr" in duration_str and "min" in duration_str:
        hours, minutes = duration_str.split("hr")
        hours = hours.strip()
        minutes = minutes.split("min")[0].strip()

    # case 2: "X hr"
    elif "hr" in duration_str:
        hours = duration_str.split("hr")[0].strip()
        minutes = 0

    # case 3: "X min"
    elif "min" in duration_str:
        hours = 0
        minutes = duration_str.split("min")[0].strip()

    else:
        print("String giving error:", duration_str)
        raise ValueError("Invalid duration string.")

    return timedelta(hours=int(hours), minutes=int(minutes))


def format_airline_correctly(airline: str) -> str:
    """
    Takes an airline name and formats it correctly.
    For example:
    - 'AironeAirtwo' --> 'Airone'
    - 'AironeOperatedBy Airtwo' --> 'Airone'
    Returns: the formatted airline name.
    """
    # case operated: XOperated by Airline --> take only airline before (X)
    case_operated = re.match(r"(.*)Operated by (.*)", airline)
    if case_operated:
        return case_operated.group(1)

    # case camel: AironeAirtwo (2 airlines separated by camel case) --> take only first airline
    case_camel = re.match(r"([A-Z][a-z]+)(\s[A-Z][a-z]+)?", airline)
    if case_camel and len(airline.split(" ")) > 1 and not airline.endswith("Air"):
        return case_camel.group(1) + (case_camel.group(2) or "")  # For example: Air DolomitiLufthansa --> Air Dolomiti

    return airline


def move_pandas_column_to_front(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """
    Moves a column in a pandas DataFrame to the front.
    Returns: DataFrame with the column moved to the front.
    """
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index(column_name)))
    return df[cols]


class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, max_filename_length=30):
        super().__init__(fmt, datefmt)
        self.max_filename_length = max_filename_length

    def format(self, record):
        # Remove .py at the end of the filename
        record.filename = record.filename.replace(".py", "")

        # Calculate the padding needed to align the filenames
        padding = " " * (self.max_filename_length - len(record.filename))
        record.filename = f"{record.filename}{padding}"
        return super().format(record)


def setup_logging(logging_config_file: str = "logging.conf"):
    """
    Setup logging configuration from the logging.conf file.

    Basically, this function reads the logging configuration from the logging.conf file and sets up the logging system.
    It also sets the level to ERROR for loggers that do not have a corresponding .py version (i.e. external modules).
    # TODO: document better

    :param logging_config_file: Path to the logging configuration file.
    """
    assert os.path.isfile(logging_config_file), "logging.conf file not found."

    logging.config.fileConfig("logging.conf", disable_existing_loggers=False)

    # Create the custom formatter
    formatter = CustomFormatter("%(asctime)s - %(filename)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S", max_filename_length=18)

    # Get the console handler from the root logger and set the custom formatter
    console_handler = logging.getLogger().handlers[0]
    console_handler.setFormatter(formatter)

    # collect all logger names
    logger_names = list(logging.root.manager.loggerDict.keys())

    # identify loggers with and without .py extension
    modules_to_disable = set()
    for logger_name in logger_names:
        if logger_name.endswith(".py"):
            base_name = logger_name[:-3]  # Remove the .py extension
            modules_to_disable.add(base_name)

    # set level to ERROR for loggers not having corresponding .py version
    for logger_name in logger_names:
        if logger_name not in modules_to_disable and not logger_name.endswith(".py"):
            logging.getLogger(logger_name).setLevel(logging.ERROR)
