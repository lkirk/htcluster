import sqlite3
from datetime import datetime
from pathlib import Path

import structlog
from dateutil import tz

from htcluster.validators_3_9_compat import RunnerPayload

LOG = structlog.get_logger("db")
ADS = [
    ("Cmd", str),
    ("Iwd", str),
    ("JobPrio", int),
    ("UserLog", str),
    ("ClusterId", int),
    ("StreamErr", int),
    ("StreamOut", int),
    ("DockerImage", str),
    ("Environment", str),
    ("JobUniverse", int),
    ("RequestCpus", int),
    ("RequestDisk", int),
    ("JobBatchName", str),
    ("Requirements", str),
    ("CondorVersion", str),
    ("RequestMemory", int),
    ("TransferInput", str),
    ("CondorPlatform", str),
    ("TransferOutput", str),
    ("JobNotification", int),
    ("JobSubmitMethod", int),
    ("LeaveJobInQueue", int),
    ("DockerPullPolicy", str),
    ("JobLeaseDuration", int),
    ("ShouldTransferFiles", str),
    ("TransferInputSizemb", int),
    ("EnteredCurrentStatus", int),
    ("WhenToTransferOutput", str),
]


def schema(con: sqlite3.Connection) -> None:
    with con:
        con.execute(
            """CREATE TABLE job_classads (
                   cmd TEXT, iwd TEXT, job_prio INTEGER, user_log TEXT,
                   cluster_id INTEGER PRIMARY KEY, stream_err INTEGER,
                   stream_out INTEGER, docker_image TEXT, environment TEXT,
                   job_universe INTEGER, request_cpus INTEGER, request_disk INTEGER,
                   job_batch_name TEXT, requirements TEXT, condor_version TEXT,
                   request_memory INTEGER, transfer_input TEXT, condor_platform TEXT,
                   transfer_output TEXT, job_notification INTEGER,
                   job_submit_method INTEGER, leave_job_in_queue INTEGER,
                   docker_pull_policy TEXT, job_lease_duration INTEGER,
                   should_transfer_files TEXT, transfer_input_sizemb INTEGER,
                   entered_current_status INTEGER, when_to_transfer_output TEXT
            ) WITHOUT ROWID"""
        )

        con.execute(
            """CREATE TABLE jobs (
                   cluster_id INTEGER PRIMARY KEY,
                   num_procs INTEGER,
                   job_name TEXT,
                   submitted_on TIMESTAMP,
                   submitted_on_tz STRING,
                   FOREIGN KEY(cluster_id) REFERENCES job_classads(cluster_id)
            ) WITHOUT ROWID
            """
        )

        con.execute(
            """CREATE TABLE procs (
                   cluster_id INTEGER,
                   in_files TEXT,
                   out_files TEXT,
                   params TEXT,
                   FOREIGN KEY(cluster_id) REFERENCES job_classads(cluster_id)
            )"""
        )

        con.execute(
            "CREATE UNIQUE INDEX job_classads_cluster_id ON job_classads(cluster_id)"
        )
        con.execute("CREATE UNIQUE INDEX jobs_cluster_id ON jobs(cluster_id)")
        con.execute("CREATE INDEX procs_cluster_id ON procs(cluster_id)")


def connect(db: Path) -> sqlite3.Connection:
    write_schema = False
    if not db.exists():
        LOG.info("database does not exist, writing schema", db=str(db))
        write_schema = True
    con = sqlite3.connect(db)
    con.execute("PRAGMA foreign_keys = ON")
    if write_schema:
        schema(con)
    return con


def ad_or_None(classads, ad, t):
    try:
        return t(classads[ad])
    except KeyError:
        return None


# TODO: can't type sub_result (htcondor2._submit_result.SubmitResult)
def write_submission_data(
    con: sqlite3.Connection, submit_result, params: RunnerPayload
) -> None:
    classads = submit_result.clusterad()
    submit_time = datetime.fromtimestamp(classads["QDate"]).astimezone(tz.tzlocal())
    local_timezone = tz.tzlocal().tzname(datetime.now())
    job_classads = [ad_or_None(classads, ad, t) for ad, t in ADS]
    jobs = (
        submit_result.cluster(),
        submit_result.num_procs(),
        params.job.name,
        submit_time,
        local_timezone,
    )
    procs = [
        (
            submit_result.cluster(),
            str(p.in_files),
            str(p.out_files),
            p.model_dump_json(),
        )
        for p in params.params
    ]
    try:
        with con:
            con.execute(
                f"INSERT INTO job_classads VALUES ({', '.join(['?'] * len(job_classads))})",
                job_classads,
            )
            con.execute(
                """UPDATE jobs AS j
                       SET cluster_id = c.cluster_id
                       FROM job_classads AS c
                       WHERE j.cluster_id = c.cluster_id
                """
            )
            con.execute(
                f"INSERT INTO jobs VALUES ({', '.join(['?'] * len(jobs))})", jobs
            )
            con.execute(
                """UPDATE procs AS p
                       SET cluster_id = c.cluster_id
                       FROM job_classads AS c
                       WHERE p.cluster_id = c.cluster_id
                """
            )
            con.executemany(
                f"INSERT INTO procs VALUES ({', '.join(['?'] * len(procs[0]))})", procs
            )
    except sqlite3.IntegrityError as e:
        LOG.exception(e)
