import argparse
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlunparse

import structlog

from htcluster.config import Config, load_config
from htcluster.job_exec.client import connect_local, connect_remote, send
from htcluster.logging import log_config
from htcluster.validator_base import BaseModel
from htcluster.validators import ClusterJob, ImplicitOut, JobSettings
from htcluster.validators_3_9_compat import JobArgs, RunnerPayload

from .github import get_most_recent_container_hash
from .ssh import chtc_ssh_client, copy_file_sftp, mkdir_sftp, write_file_sftp

log_config()
LOG = structlog.get_logger()


# Standard job sub-directories
LOG_DIR = Path("log")
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")


# Path on remote server (relative to homedir) where results will be stored
CLUSTER_DIR = Path("analysis-results")


def strip_suffixes(name: str) -> Path:
    return Path(re.split(r"\.", name)[0])


def get_implicit_out(path: Path | str | int, suffix: str) -> Path:
    return strip_suffixes(str(path)).with_suffix(suffix)


def get_implicit_out_files(cj: ClusterJob) -> list[Path]:
    out_files = []
    assert isinstance(cj.params.out_files, ImplicitOut)  # mypy
    suffix = cj.params.out_files.suffix
    for j in range(cj.params.n_jobs):
        if len(cj.params.in_files) > 0:
            out_files.append(get_implicit_out(cj.params.in_files[j].name, suffix))
        else:
            out_files.append(get_implicit_out(j, suffix))
    return out_files


def file_url(path: Path) -> str:
    scheme = "file"
    netloc, params, query, fragment = ("", "", "", "")
    return urlunparse((scheme, netloc, str(path), params, query, fragment))


def get_input_output_dirs(
    job_params: JobSettings, ssh_remote_user: str, job_dir: Path
) -> tuple[Path | None, Path, Path, Path, Path]:
    if job_params.in_staging or job_params.out_staging:
        staging_dir = Path("/staging") / ssh_remote_user / job_params.name
        input_dir = staging_dir / INPUT_DIR if job_params.in_staging else INPUT_DIR
        output_dir = staging_dir / OUTPUT_DIR if job_params.out_staging else OUTPUT_DIR
        in_job_dir = input_dir if job_params.in_staging else job_dir / INPUT_DIR
        out_job_dir = output_dir if job_params.out_staging else job_dir / OUTPUT_DIR
        return staging_dir, input_dir, output_dir, in_job_dir, out_job_dir
    return None, INPUT_DIR, OUTPUT_DIR, job_dir / INPUT_DIR, job_dir / OUTPUT_DIR


def get_runner_payload(
    cj: ClusterJob, input_dir: Path, output_dir: Path, job_dir: Path
) -> RunnerPayload:
    # resolve implicit out files (if any)
    # at this point, the outfiles are only filenames (we validate the input)
    if isinstance(cj.params.out_files, ImplicitOut):
        out_names = get_implicit_out_files(cj)
    else:
        out_names = cj.params.out_files

    have_in_files = len(cj.params.in_files) > 0
    params = [
        JobArgs(
            in_files=Path(cj.params.in_files[j].name) if have_in_files else None,
            out_files=out_names[j],
            params=(
                {k: cj.params.params[k][j] for k in cj.params.params}
                if cj.params.params is not None
                else None
            ),
        )
        for j in range(cj.params.n_jobs)
    ]

    in_files, out_files, in_files_staging, out_files_staging = [], [], [], []
    if cj.job.in_staging:
        in_files_staging = [file_url(input_dir / f.name) for f in cj.params.in_files]
    else:
        in_files = [input_dir / f.name for f in cj.params.in_files]
    if cj.job.out_staging:
        out_files_staging = [file_url(output_dir / f.name) for f in out_names]
    else:
        out_files = [output_dir / f.name for f in out_names]

    payload = RunnerPayload(
        job=cj.job,
        job_dir=job_dir,
        out_dir=output_dir,
        log_dir=LOG_DIR,
        params=params,
        # staging directories are urls
        in_files=in_files,
        out_files=out_files,
        in_files_staging=in_files_staging,
        out_files_staging=out_files_staging,
    )

    return payload


class SubmissionData(BaseModel):
    # copy of the parsed yaml obj that produced this data
    cj: ClusterJob
    # data to be sent to job execution server
    payload: RunnerPayload
    # directory to which input files will be copied (if any inputs)
    job_input_dir: Path
    # directories to be made in preparation for job submission (outputs, logs, etc)
    remote_dirs: list[Path]
    # staging directory to store inputs or outputs (if specified)
    staging_dir: Path | None


def get_submission_data(cj: ClusterJob, config: Config) -> SubmissionData:
    cluster_dir = CLUSTER_DIR

    # hit the github api to find the most recent container hash it's more
    # reliable to specify the exact container hash to be run on the cluster
    container_hash = get_most_recent_container_hash(cj.job.docker_image, config)
    cj.job.docker_image = f"{cj.job.docker_image}@{container_hash}"

    job_dir = cluster_dir / cj.job.name
    staging_dir, input_dir, output_dir, job_input_dir, job_output_dir = (
        get_input_output_dirs(cj.job, config.ssh_remote_user, job_dir)
    )

    runner_payload = get_runner_payload(cj, input_dir, output_dir, job_dir)
    remote_dirs = [job_dir, job_dir / LOG_DIR, job_input_dir, job_output_dir]

    return SubmissionData(
        cj=cj,
        payload=runner_payload,
        job_input_dir=job_input_dir,
        remote_dirs=remote_dirs,
        staging_dir=staging_dir,
    )


class MockSshClient:
    @contextmanager
    def open_sftp(self):
        yield


def print_mkdir(client: None, d: Path) -> None:
    print(f"mkdir {d}", file=sys.stderr)


def print_copy_file(client: None, src: Path, dest: Path) -> None:
    print(f"copy {src} -> {dest}", file=sys.stderr)


def print_write_file(client: None, dest: Path, data: str) -> None:
    print(f"write {dest}", file=sys.stderr)


def copy_files_prep_dirs(sub: SubmissionData, config: Config, dry_run: bool) -> None:
    """
    Using the paramiko sftp client, make all of the required output directories
    and copy inputs (if any).

    TODO: figure out how to use rsync (it's much faster for large file copies)

    NB: we mock the ssh client and file copies if we're performing a dry
    run. this helps to keep the logic up to date if anything changes.
    """
    if dry_run:
        client = MockSshClient()
        mkdir = print_mkdir
        copy_file = print_copy_file
        write_file = print_write_file
    else:
        LOG.info(
            "connecting via ssh to remote server",
            user=config.ssh_remote_user,
            server=config.ssh_remote_server,
        )
        client = chtc_ssh_client(config.ssh_remote_user, config.ssh_remote_server)
        mkdir = mkdir_sftp
        copy_file = copy_file_sftp
        write_file = write_file_sftp

    with client.open_sftp() as sftp:
        if sub.staging_dir:
            mkdir(sftp, sub.staging_dir)  # type: ignore
        for d in sub.remote_dirs:
            mkdir(sftp, d)  # type: ignore
        write_file(
            sftp,  # type: ignore
            sub.payload.job_dir / "params.json",
            sub.payload.model_dump_json(indent=2),
        )
        for j, params in enumerate(sub.payload.params):
            if params.in_files and sub.cj.params.in_files:
                copy_file(
                    sftp,  # type: ignore
                    sub.cj.params.in_files[j],
                    sub.job_input_dir / params.in_files,
                )
                # TODO: logging
                if not dry_run:
                    print(f"copied {sub.cj.params.in_files[j]}")


def send_submission_data(
    sub: SubmissionData, config: Config, test_local: bool, dry_run: bool
) -> None:
    # if we're not running a local server, do nothing
    if dry_run and not test_local:
        return

    # if we're testing locally, connect to a local instance of the execution
    # server and send the payload
    if test_local:
        socket = connect_local(config.zmq_bind_port)
        send(socket, sub.payload)
        return

    # connect to remote server through an ssh tunnel
    socket = connect_remote(
        config.zmq_bind_port, config.ssh_remote_user, config.ssh_remote_server
    )
    # send the job submission data to the remote server as gzipped json
    send(socket, sub.payload)


def load_job_yaml(job_yaml: Path | str) -> ClusterJob:
    """
    Takes three different input types:

    1. string path to yaml file
    2. yaml document in string form
    3. path object encoding the location of the yaml file

    If a string is provided, we check if it's a file by testing if the string
    encodes a file that exists, otherwise we attempt to load the string as json.
    """
    match job_yaml:
        case Path():
            cj = ClusterJob.from_yaml_file(job_yaml)
        case str():
            if not (job_yaml_path := Path(job_yaml)).exists():
                try:
                    cj = ClusterJob.from_yaml_str(job_yaml)
                except TypeError as te:
                    raise ValueError(
                        f"failed to load {job_yaml} as a string because it's not a file"
                    ) from te
            else:
                cj = ClusterJob.from_yaml_file(job_yaml_path)
        case _:
            raise ValueError(
                "Expected 'job_yaml' to be a 'str' or "
                f"'pathlib.Path', got {type(job_yaml)}"
            )
    return cj


def run_cluster_job(
    job_yaml: Path | str, dry_run=False, test_local=False, one_job=False
) -> SubmissionData:
    """
    Main entrypoint for running jobs.

    Load the job yaml, copy files to the submit server, and send a request to
    the job executor to submit the job to htcondor. We can specify if this is a
    "dry run" to see what the submission would look like. In addition, if we
    have a local execution server running with "--dry-run", we can submit to the
    test server.

    NB: To run this, you'll need a config file in place (see docs).

    :param job_yaml: Path to yaml file or a valid yaml string
    :param dry_run: Print actions that would be performed
    :param test_local: Submit jobs to a local test server
    :param one_job: Print the JobArgs for the first job
    :returns: Parsed and transformed submission data

    """
    config = load_config()
    LOG.info("start", dry_run=dry_run, test_local=test_local)
    job_descr = load_job_yaml(job_yaml)
    sub_data = get_submission_data(job_descr, config)

    if not dry_run:
        copy_files_prep_dirs(sub_data, config, dry_run=False)
        send_submission_data(sub_data, config, test_local=False, dry_run=False)
    else:
        copy_files_prep_dirs(sub_data, config, dry_run=True)
        send_submission_data(sub_data, config, test_local=test_local, dry_run=True)
        if one_job:
            print(sub_data.payload.params[0].model_dump_json(indent=2))
        else:
            print(sub_data.payload.model_dump_json(indent=2))

    return sub_data


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_yaml", type=Path, help="input job description yaml.")
    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="do not perform any actions, just gather data and validate inputs.",
    )
    parser.add_argument(
        "--one-job",
        action="store_true",
        help=(
            "to be used with --dry-run; print JobArgs for the first job, for testing "
            "the job wrapper locally."
        ),
    )
    parser.add_argument(
        "--test-local",
        action="store_true",
        help=(
            "to be used with --dry-run; test against a local server instance, do "
            "not use ssh."
        ),
    )
    args = parser.parse_args()
    if (not args.dry_run) and args.test_local:
        parser.error(
            "--dry-run is not specified, but --test-local is. specify --dry-run.",
        )
    if (not args.dry_run) and args.one_job:
        parser.error(
            "--dry-run is not specified, but --one-job is. specify --dry-run.",
        )
    return args


def main():
    """
    Main entrypoint for the job submission script
    """
    args = parse_args()

    if not args.job_yaml.exists() and args.job_yaml.is_file():
        raise ValueError(f"{args.job_yaml} does not exist")

    run_cluster_job(args.job_yaml, args.dry_run, args.test_local, args.one_job)


if __name__ == "__main__":
    main()
