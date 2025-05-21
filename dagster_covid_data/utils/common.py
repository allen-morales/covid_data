from datetime import datetime, timedelta

def generate_dates(start_date: str, end_date: str) -> list:
    """
    Generates all dates in the format mm-dd-yyyy between the given start and end dates.

    Args:
        start_date (str): The start date in the format mm-dd-yyyy.
        end_date (str): The end date in the format mm-dd-yyyy.

    Returns:
        list: A list of dates in the format mm-dd-yyyy.
    """
    # Convert input strings to datetime objects
    start = datetime.strptime(start_date, "%m-%d-%Y")
    end = datetime.strptime(end_date, "%m-%d-%Y")
    
    # Generate all dates between start and end
    dates = []
    current_date = start
    while current_date <= end:
        dates.append(current_date.strftime("%m-%d-%Y"))
        current_date += timedelta(days=1)
    
    return dates

