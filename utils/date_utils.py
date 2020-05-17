from datetime import datetime


def parse_datetime(date_string):
    """ Get the expected time string into datetime object.
    """
    # Unpact date, and time if it exists
    (date, *time) = date_string.split(' ')
    if time:
        return datetime.strptime(date_string, '%m/%d/%y %H:%M')
    else:
        return datetime.strptime(date_string, '%m/%d/%y')
