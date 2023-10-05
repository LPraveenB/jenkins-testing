import multiprocessing

logger = None


def get_multiprocessing_pool():
    """
    The get_multiprocessing_pool function creates a multiprocessing pool with the number of processes equal to the total
    number of CPUs on your machine. This is useful for parallelize tasks that can be done in parallel, such as
    downloading files from S3 or running multiple simulations at once.

    :return: A multiprocessing pool object
    """
    try:
        total_cpu = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(total_cpu)
        return pool

    except Exception as e:
        logger.error(f"Error occurred while creating multiprocessing pool: {e}")
        raise


def close_multiprocessing_pool(pool: object):
    """
    The close_multiprocessing_pool function closes the multiprocessing pool.

    :param pool: object: Pass in the multiprocessing pool object
    """
    try:
        pool.close()

    except Exception as e:
        logger.error(f"Error occurred while closing multiprocessing pool: {e}")
        raise


def join_multiprocessing_pool(pool: object):
    """
    The join_multiprocessing_pool function is used to join the multiprocessing pool.

    :param pool: object: Specify the multiprocessing pool that is being joined
    """
    try:
        pool.join()

    except Exception as e:
        logger.error(f"Error occurred while joining multiprocessing pool: {e}")
        raise
