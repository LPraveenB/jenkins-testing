from datetime import datetime

logger = None


def remove_hyphens(strings: list):
    """
    The remove_hyphens function takes a list of strings and removes hyphens from each string.
        Args:
            strings (list): A list of strings to remove hyphens from.

    :param strings: list: Tell the function that it will be receiving a list of strings
    :return: A list of strings without hyphens
    """
    try:
        return list(map(lambda string: string.replace('-', ''), strings))
    except Exception as e:
        logger.error(f"Error while removing hyphens from list of strings : {e}")
        raise


def sort_date_strings(date_strings: list, date_format="%d/%m/%Y"):
    """
    The sort_date_strings function takes a list of date strings and sorts them in ascending order.
    The function returns the sorted list of date strings.


    :param date_strings: list: Pass in a list of date strings
    :param date_format: Specify the format of the date strings in the list
    :return: A list of sorted dates
    """
    try:
        sorted_dates = sorted(date_strings, key=lambda x: datetime.strptime(x, date_format))
        return sorted_dates
    except Exception as e:
        logger.error(f"Error occurred while sorting date strings: {str(e)}")
        raise
