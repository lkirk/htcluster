import argparse
import re
from pathlib import Path
from urllib.parse import urlunparse

from htcluster.config import load_config
from htcluster.job_exec.client import connect_local, connect_remote, send
from htcluster.logging import log_config
from htcluster.validators import ClusterJob, ImplicitOut, JobSettings
from htcluster.validators_3_9_compat import JobArgs, RunnerPayload

from .github import get_most_recent_container_hash
from .ssh import chtc_ssh_client, copy_file, mkdir
from .yaml import read_and_validate_job_yaml

log_config()

# Standard job sub-directories
LOG_DIR = Path("log")
INPUT_DIR = Path("input")
OUTPUT_DIR = Path("output")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_yaml", type=Path, help="input job description yaml")
    parser.add_argument(
        "--dry-run",
        "-d",
        action="store_true",
        help="do not perform any actions, just gather data and validate inputs",
    )
    parser.add_argument(
        "--test-local",
        action="store_true",
        help="test against a local server instance, do not use ssh",
    )
    return parser.parse_args()


def strip_suffixes(name: str) -> Path:
    return Path(re.split(r"\.", name)[0])


def get_implicit_out(path: Path | str | int, suffix: str) -> Path:
    return strip_suffixes(str(path)).with_suffix(suffix)


def get_implicit_out_files(cj: ClusterJob) -> list[Path]:
    out_files = []
    assert isinstance(cj.params.out_files, ImplicitOut)  # mypy
    suffix = cj.params.out_files.suffix
    for j in range(cj.n_jobs):
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
        for j in range(cj.n_jobs)
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


def main():
    args = parse_args()
    config = load_config()
    cluster_dir = Path("analysis-results")

    if not args.job_yaml.exists() and args.job_yaml.is_file():
        raise ValueError(f"{args.job_yaml} does not exist")

    job_descr = read_and_validate_job_yaml(args.job_yaml)

    container_hash = get_most_recent_container_hash(job_descr.job.docker_image, config)
    job_descr.job.docker_image = f"{job_descr.job.docker_image}@{container_hash}"

    job_dir = cluster_dir / job_descr.job.name
    staging_dir, input_dir, output_dir, job_input_dir, job_output_dir = (
        get_input_output_dirs(job_descr.job, config.ssh_remote_user, job_dir)
    )

    runner_payload = get_runner_payload(job_descr, input_dir, output_dir, job_dir)
    make_remote_dirs = [staging_dir] if staging_dir else []
    make_remote_dirs.extend([job_dir, job_dir / LOG_DIR, job_input_dir, job_output_dir])
    if not args.dry_run:
        client = chtc_ssh_client(config.ssh_remote_user, config.ssh_remote_server)
        with client.open_sftp() as sftp:
            for d in make_remote_dirs:
                mkdir(sftp, d)
            for j, params in enumerate(runner_payload.params):
                if params.in_files and job_descr.params.in_files:
                    copy_file(
                        sftp,
                        job_descr.params.in_files[j],
                        job_input_dir / params.in_files,
                    )
                    # TODO: logging
                    print(f"copied {job_descr.params.in_files[j]}")

        socket = connect_remote(
            config.zmq_bind_port, config.ssh_remote_user, config.ssh_remote_server
        )
        send(socket, runner_payload)
    else:
        if args.test_local:
            socket = connect_local(config.zmq_bind_port)
            send(socket, runner_payload)
        else:
            import sys

            for d in make_remote_dirs:
                print(f"mkdir {d}", file=sys.stderr)
            for j, params in enumerate(runner_payload.params):
                if params.in_files and job_descr.params.in_files:
                    print(
                        f"copy {job_descr.params.in_files[j]} -> {job_dir / input_dir / params.in_files}",
                        file=sys.stderr,
                    )

            print(runner_payload.model_dump_json(indent=2))

        # write_file(
        #     sftp,
        #     input_dir / f"params_{j}.json",
        #     json.dumps(params, indent=2),
        # )
        # print(f"wrote params for job {j}")


if __name__ == "__main__":
    main()
