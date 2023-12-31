import re
from datetime import date, datetime, timedelta


def check_date_in_future(input_date: date) -> date:
    """
    Checks if a date is in the future.
    Returns: Date.
    """
    if input_date < date.today():
        raise ValueError("Date must be in the future.")
    
    return input_date

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
    datetime_obj = datetime.strptime(date_time_str, '%Y-%m-%d %I:%M%p') + delta
    
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
    case_operated = re.match(r'(.*)Operated by (.*)', airline)
    if case_operated:
        return case_operated.group(1)
    
    # case camel: AironeAirtwo (2 airlines separated by camel case) --> take only first airline
    case_camel = re.match(r'([A-Z][a-z]+)(\s[A-Z][a-z]+)?', airline)
    if case_camel and len(airline.split(" ")) > 1 and not airline.endswith("Air"):
        return case_camel.group(1) + (case_camel.group(2) or '') # For example: Air DolomitiLufthansa --> Air Dolomiti
    
    
    return airline
