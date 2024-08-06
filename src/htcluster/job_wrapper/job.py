from functools import wraps

from htcluster.logging import log_config


def job_wrapper(schema):
    """
    This wrapper validates the input parameters with a user defined
    schema and initialize the logger, configuring it to print to stderr

    Usage:

    .. code-block:: python
    @job_wrapper(MySchema)
    def main(args: MySchema) -> None:
        ...
    """

    def inner(func):
        @wraps(func)
        def wrapper(job_params):
            log_config()
            # load the validated JobArgs with (presumably) narrower types specified
            # in the job workflow entrypoint
            valid_job_params = schema.model_validate(job_params, from_attributes=True)
            return func(valid_job_params)

        return wrapper

    return inner
