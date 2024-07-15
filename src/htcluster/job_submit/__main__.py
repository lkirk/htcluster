import argparse
import re
from pathlib import Path

from htcluster.job_exec.client import connect_local, connect_remote, send
from htcluster.logging import log_config
from htcluster.validators import ClusterJob, ImplicitOut, ProgrammaticJobParams
from htcluster.validators_3_9_compat import JobArgs, RunnerPayload

from .github import get_most_recent_container_hash
from .ssh import chtc_ssh_client, copy_file, mkdir
from .yaml import read_and_validate_job_yaml

log_config()


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


def get_implicit_out_files(cj: ClusterJob, output_dir: Path) -> list[Path]:
    out_files = []
    assert isinstance(cj.params, ProgrammaticJobParams)  # mypy
    assert isinstance(cj.params.out_files, ImplicitOut)  # mypy
    suffix = cj.params.out_files.suffix
    for j in range(cj.n_jobs):
        if len(cj.params.in_files) > 0:
            out_files.append(
                output_dir / get_implicit_out(cj.params.in_files[j].name, suffix)
            )
        else:
            out_files.append(output_dir / get_implicit_out(j, suffix))
    return out_files


def get_per_job_params(
    cj: ClusterJob, input_dir: Path, out_files: list[Path]
) -> list[JobArgs]:
    params = []
    in_files = []
    # TODO: grouped job params
    assert isinstance(cj.params, ProgrammaticJobParams)  # mypy

    match (len(cj.params.in_files) > 0, cj.params.params is not None):
        case (True, True):
            for j in range(cj.n_jobs):
                assert cj.params.params is not None  # mypy
                in_files.append(input_dir / cj.params.in_files[j].name)
                params.append(
                    JobArgs(
                        in_files=Path(cj.params.in_files[j].name),
                        out_files=Path(out_files[j].name),
                        params={k: cj.params.params[k][j] for k in cj.params.params},
                    )
                )
        case (False, True):
            for j in range(cj.n_jobs):
                assert cj.params.params is not None  # mypy
                params.append(
                    JobArgs(
                        params={k: cj.params.params[k][j] for k in cj.params.params},
                        out_files=Path(out_files[j].name),
                    )
                )
        case (True, False):
            for j in range(cj.n_jobs):
                params.append(
                    JobArgs(
                        in_files=input_dir / cj.params.in_files[j].name,
                        out_files=Path(out_files[j].name),
                    )
                )

    return params


def main():
    args = parse_args()
    cluster_dir = Path("analysis-results")

    if not args.job_yaml.exists() and args.job_yaml.is_file():
        raise ValueError(f"{args.job_yaml} does not exist")

    job_descr = read_and_validate_job_yaml(args.job_yaml)

    container_hash = get_most_recent_container_hash(job_descr.job.docker_image)
    job_descr.job.docker_image = f"{job_descr.job.docker_image}@{container_hash}"

    job_dir = cluster_dir / job_descr.job.name
    input_dir = Path("input")
    output_dir = Path("output")
    log_dir = Path("log")

    assert isinstance(job_descr.params, ProgrammaticJobParams)
    if isinstance(job_descr.params.out_files, ImplicitOut):
        out_files = get_implicit_out_files(job_descr, output_dir)
    else:
        out_files = job_descr.params.out_files
    job_params = get_per_job_params(job_descr, input_dir, out_files)

    runner_payload = RunnerPayload(
        job=job_descr.job,
        job_dir=job_dir,
        out_dir=output_dir,
        log_dir=log_dir,
        params=job_params,
        out_files=out_files,
        in_files=[input_dir / f.name for f in job_descr.params.in_files],
    )

    if not args.dry_run:
        client = chtc_ssh_client()
        with client.open_sftp() as sftp:
            mkdir(sftp, job_dir)
            mkdir(sftp, job_dir / input_dir)
            mkdir(sftp, job_dir / output_dir)
            mkdir(sftp, job_dir / log_dir)
            assert isinstance(job_descr.params, ProgrammaticJobParams)  # mypy
            for j, params in enumerate(runner_payload.params):
                if (
                    params.in_files is not None
                    and job_descr.params.in_files is not None
                ):
                    copy_file(
                        sftp,
                        job_descr.params.in_files[j],
                        job_dir / input_dir / params.in_files,
                    )
                    print(f"copied {job_descr.params.in_files[j]}")

        # TODO: config
        socket = connect_remote(5555, "lkirk2", "ap2002.chtc.wisc.edu")
        send(socket, runner_payload)
    else:
        if args.test_local:
            # TODO: config
            socket = connect_local(5555)
            send(socket, runner_payload)
        else:
            print(runner_payload.model_dump_json(indent=2))

        # write_file(
        #     sftp,
        #     input_dir / f"params_{j}.json",
        #     json.dumps(params, indent=2),
        # )
        # print(f"wrote params for job {j}")


if __name__ == "__main__":
    main()
