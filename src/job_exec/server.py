import argparse
import gzip
from typing import Any, Optional

import structlog
import zmq
from htcondor import htcondor
from pydantic import ValidationError

from htcluster.validators_3_9_compat import RunnerPayload

LOG = structlog.get_logger()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=5555,
        help="port for zmq to bind and listen on",
    )
    parser.add_argument(
        "--json-logging",
        action="store_true",
        default=False,
        help="json log output",
    )
    parser.add_argument(
        "--debug-logging",
        action="store_true",
        default=False,
        help="debugging log output",
    )
    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="do not perform any actions, just gather data and validate inputs",
    )
    return parser.parse_args()


def parse_message(raw_message) -> Optional[RunnerPayload]:
    payload = None
    try:
        message = gzip.decompress(raw_message)
        LOG.debug("received message")
        try:
            payload = RunnerPayload.model_validate_json(message)
        except ValidationError as e:
            LOG.exception(e)
    except gzip.BadGzipFile:
        LOG.info("message not gzipped", message=raw_message)
    return payload


BASE_SUBMISSION = {
    "universe": "docker",
    "docker_pull_policy": "always",
    "output": "$(OutputDir)/logs/out/$(Process).log",
    "error": "$(OutputDir)/logs/err/$(Process).log",
    "log": "$(Cluster).log",
}


def make_submission(
    payload: RunnerPayload,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    # TODO: job name?
    itemdata: list[dict[str, str]] = []
    sub: dict[str, Any] = BASE_SUBMISSION.copy()

    if payload.job.classads is not None:
        sub.update({"requirements": payload.job.classads})

    sub.update(
        {
            "JobBatchName": payload.job.name,
            "request_memory": payload.job.memory,
            "request_cpus": payload.job.cpus,
            "request_disk": payload.job.disk,
            "arguments": f"{payload.job.entrypoint} '$(job_json)'",
        }
    )
    for p in payload.params:
        itemdata.append({"job_json": p.model_dump_json()})

    if len(payload.in_files) > 0:
        sub.update({"transfer_input_files": "$(in_file)"})
        for j in range(len(payload.params)):
            itemdata[j].update({"in_file": str(payload.in_files[j])})

    if len(payload.out_files) > 0:
        sub.update(
            {
                "should_transfer_files": "YES",
                "transfer_output_files": "ON_EXIT",
                "transfer_output_files": "$(job_out_file)",
                "transfer_output_remaps": "$(job_out_file) = $(out_file)",
            }
        )
        for j in range(len(payload.params)):
            itemdata[j].update(
                {
                    "job_out_file": str(payload.params[j].out_files),
                    "out_file": str(payload.out_files[j]),
                }
            )

    return sub, itemdata


def main():
    args = parse_args()

    if args.json_logging:
        structlog.configure(processors=[structlog.processors.JSONRenderer()])

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    addr = f"tcp://*:{args.port}"
    socket.bind(addr)
    LOG.info("listening", addr=addr)

    while True:
        m = parse_message(socket.recv())
        socket.send(b"ack")
        if m is not None:
            LOG.info(f"parsed job data for job: {m.job.name}")
            sub, itemdata = make_submission(m)
            LOG.info(f"submitting {sub}")
            if args.dry_run is False:
                schedd = htcondor.Schedd(htcondor.Submit(sub), itemdata=itemdata)
                import IPython

                IPython.embed()
            else:
                LOG.info(f"itemdata {itemdata}")


if __name__ == "__main__":
    main()
