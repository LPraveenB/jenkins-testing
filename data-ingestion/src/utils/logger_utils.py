import logging


def setup_logger(log_level: str, log_name: str):
    """
    The setup_logger function creates a logger with the specified name and logging level.
        Args:
            log_level (str): The string representation of the logging level.
                Expected values are DEBUG, INFO, WARNING, ERROR or CRITICAL.

    :param log_level: str: Set the logging level dynamically
    :param log_name: str: Name the logger
    :return: A logger object
    """
    try:
        # Get the logging level dynamically
        logging_level = getattr(logging, log_level)

        # Create a logger
        logger = logging.getLogger(log_name)
        logger.setLevel(logging_level)

        # Create a handler and formatter
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)
        return logger
    except Exception as e:
        print(f"The logger has not been created : {e}")
