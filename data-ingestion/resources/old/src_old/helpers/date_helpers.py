from datetime import datetime, timedelta

logger = None


def get_date(offset: int, date_format="%Y-%m-%d"):
    """
    The get_date function takes an offset and a date_format as arguments.
        The offset is used to calculate the date based on today's date.
        If the offset is positive, it will add that many days to today's date.
        If the offset is negative, it will subtract that many days from today's
            date.

    :param offset: int: Specify the number of days to offset from today's date
    :param date_format: Format the date
    :return: A string representation of a date in the format specified by the date_format parameter
    """
    try:
        if offset > 0:
            date = datetime.now().date() + timedelta(days=offset - 1)
        else:
            date = datetime.now().date() - timedelta(days=abs(offset))
        return date.strftime(date_format)
    except Exception as e:
        logger.error(f"Error occurred while getting date: {str(e)}")
        raise


def convert_date_formats(date_string: str, input_format: str, date_formats: list):
    """
    The convert_date_formats function takes a date string, an input format, and a list of output formats.
    It then converts the date string to datetime object using the input format. It then iterates through each
    output format in the list of output formats and converts it to that specific output format.

    :param date_string: str: Pass in the date string to be converted
    :param input_format: str: Specify the format of the date_string parameter
    :param date_formats: list: Pass in a list of date formats that the input_date should be converted to
    :return: A list of strings
    """
    try:
        input_date = datetime.strptime(date_string, input_format)
        converted_dates = []
        for format_str in date_formats:
            converted_date = input_date.strftime(format_str)
            converted_dates.append(converted_date)
        return converted_dates
    except Exception as e:
        logger.error(f"Error occurred during date conversion: {e}")
        raise
