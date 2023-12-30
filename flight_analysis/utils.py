from datetime import date, datetime


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