from functools import wraps


from htcluster.logging import log_config


def job_wrapper(schema):
    """
    This wrapper validates the input parameters with a user defined
    schema and initialize the logger, configuring it to print to stderr

    Usage:

    @job_wrapper(MySchema)
    def main(args: MySchema) -> None:
        ...
    """

    def inner(func):
        @wraps(func)
        def wrapper(job_params):
            log_config()
            valid_job_params = schema(**job_params)
            return func(valid_job_params)
        return wrapper
    return inner
