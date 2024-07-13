from importlib import import_module

import structlog
from htcluster.validators import JobArgs


import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("module", help="module to run")
    parser.add_argument("json_args", help="arguments to module, json encoded")
    return parser.parse_args()


def main():
    cli_args = parse_args()
    log = structlog.get_logger()
    log.info(f"starting")

    args = JobArgs.model_validate_json(cli_args.json_args)

    module, function = cli_args.module.split(":")
    entrypoint = getattr(import_module(module), function)

    log.info("running job", entrypoint=cli_args.module, args=args.model_dump())
    entrypoint(args)


if __name__ == "__main__":
    main()
