import argparse
from importlib import import_module

import structlog

from htcluster.logging import log_config
from htcluster.validators_3_9_compat import JobArgs


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("module", help="module to run")
    parser.add_argument("json_args", help="arguments to module, json encoded")
    return parser.parse_args()


def main():
    cli_args = parse_args()
    log_config()
    log = structlog.get_logger(module="job_wrapper")
    log.info("starting")

    args = JobArgs.model_validate_json(cli_args.json_args)

    module, function = cli_args.module.split(":")
    entrypoint = getattr(import_module(module), function)

    log.info(
        "running job",
        entrypoint=cli_args.module,
        args={k: str(v) for k, v in args.model_dump().items()},
    )
    entrypoint(args)


if __name__ == "__main__":
    main()
