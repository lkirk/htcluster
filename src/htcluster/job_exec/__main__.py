import argparse
import gzip
import signal
import sqlite3
import sys
from pathlib import Path
from types import FrameType
from typing import Any, Optional

import htcondor2 as htcondor
import structlog
import zmq
from pydantic import ValidationError

from htcluster.validators_3_9_compat import RunnerPayload

from . import db

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
    parser.add_argument(
        "--db-path", type=Path, default=Path("~/.local/var/job_exec.db").expanduser()
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


def make_submission(
    payload: RunnerPayload,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    itemdata = []
    sub = {
        "universe": "docker",
        "initialdir": Path("~").expanduser() / payload.job_dir,
        "JobBatchName": payload.job.name,
        "docker_image": payload.job.docker_image,
        "request_memory": payload.job.memory,
        "request_cpus": payload.job.cpus,
        "request_disk": payload.job.disk,
        "arguments": f"{payload.job.entrypoint} $(job_json)",
        "output": payload.log_dir / "out/$(Process).log",
        "error": payload.log_dir / "err/$(Process).log",
        "log": payload.log_dir / "cluster.log",
    }
    if payload.job.additional_args is not None:
        for arg_name, arg in payload.job.additional_args.items():
            sub[arg_name] = arg

    classads = payload.job.classads
    if payload.job.in_staging or payload.job.out_staging:
        # Ensure that the target has access to staging if we're using it
        if classads:
            # TODO: could use a regex to avoid extra parentheses
            classads = f"({classads}) && (Target.HasCHTCStaging == true)"
        else:
            classads = "(Target.HasCHTCStaging == true)"

    if classads:
        sub.update({"requirements": classads})

    for p in payload.params:
        itemdata.append({"job_json": p.model_dump_json().replace('"', r"\"")})

    if payload.has_inputs():
        sub.update({"transfer_input_files": "$(in_file)"})
        for j in range(len(payload.params)):
            itemdata[j].update({"in_file": str(payload.get_in_file(j))})

    # we currently don't allow jobs to not have outputs in the submission script
    if payload.has_outputs():
        sub.update(
            {
                "should_transfer_files": "YES",
                "when_to_transfer_output": "ON_EXIT",
                "transfer_output_files": "$(job_out_file)",
                "transfer_output_remaps": '"$(job_out_file) = $(out_file)"',
            }
        )
        for j in range(len(payload.params)):
            itemdata[j].update(
                {
                    "job_out_file": str(payload.params[j].out_files),
                    "out_file": str(payload.get_out_file(j)),
                }
            )

    return sub, itemdata


def register_signal_handler(
    context: zmq.Context, job_db: sqlite3.Connection, signals: list[signal.Signals]
) -> None:
    def shutdown_handler(signum: int, frame: Optional[FrameType]) -> None:
        LOG.info(
            "received shutdown signal, shutting down",
            signal=signal.Signals(signum).name,
        )
        context.destroy()
        job_db.close()
        sys.exit(0)

    for sig in signals:
        signal.signal(sig, shutdown_handler)


def bind_socket(uri: str) -> tuple[zmq.Context, zmq.SyncSocket]:
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(uri)
    return context, socket


def serve_forever(
    socket: zmq.SyncSocket, job_db: sqlite3.Connection, dry_run: bool
) -> None:
    while True:
        m = parse_message(socket.recv())
        socket.send(b"ack")
        if m is not None:
            LOG.info(f"parsed job data for job: {m.job.name}")
            try:
                sub, itemdata = make_submission(m)
            except Exception as e:
                LOG.exception(e)
                continue

            LOG.info(f"submitting {sub}")
            if dry_run is False:
                submission = htcondor.Submit(sub)
                submission.issue_credentials()

                schedd = htcondor.Schedd()
                try:
                    result = schedd.submit(submission, itemdata=iter(itemdata))
                    db.write_submission_data(job_db, result, m)
                    LOG.info("wrote job data to db")
                except Exception as e:
                    LOG.exception(e)
            else:
                LOG.info("printing data", itemdata=str(itemdata)[0:1024])


def main():
    args = parse_args()

    if args.json_logging:
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
                structlog.processors.dict_tracebacks,
                structlog.processors.JSONRenderer(),
            ]
        )

    if not (db_parent := args.db_path.parent).exists():
        LOG.info("db path does not exist, creating", path=str(db_parent))
        db_parent.mkdir(parents=True)

    job_db = db.connect(args.db_path)
    LOG.info("connected to job database", db=str(args.db_path))

    uri = f"tcp://*:{args.port}"
    context, socket = bind_socket(uri)
    register_signal_handler(context, job_db, [signal.SIGINT, signal.SIGTERM])
    serve_forever(socket, job_db, args.dry_run)
    LOG.info("listening", addr=uri)


if __name__ == "__main__":
    main()
