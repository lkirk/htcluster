from functools import wraps

from htcluster.logging import log_config
from htcluster.validator_base import BaseModel


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
            # load the validated JobArgs with (presumably) narrower types specified
            # in the job workflow entrypoint
            match job_params:
                case BaseModel():
                    valid = schema.model_validate(job_params, from_attributes=True)
                case dict():
                    valid = schema(**job_params)
                case _:
                    raise ValueError(
                        f"job_params has unexpected type: {type(job_params)}, "
                        "expected an instance of BaseModel or dict"
                    )
            return func(valid)

        return wrapper

    return inner
