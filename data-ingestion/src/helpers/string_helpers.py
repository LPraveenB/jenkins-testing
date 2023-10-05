import ast

logger = None


def convert_string_to_list(string):
    """
    The convert_string_to_list function takes a string as input and returns a list.
        The function is used to convert the string representation of lists in the dataframe into actual lists.

    :param string: Convert the string to a list
    :return: A list
    """
    try:
        result = ast.literal_eval(string)
        if isinstance(result, list):
            return result
        else:
            raise
    except Exception as e:
        logger.error(f"Error occurred while converting string to list: {str(e)}")
        raise
