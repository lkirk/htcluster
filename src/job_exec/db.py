from pathlib import Path

import structlog
import sqlite3

# BEGIN TRANSACTION; INSERT INTO job_classads VALUES (NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL); UPDATE jobs AS j SET cluster_id = c.cluster_id FROM job_classads AS c WHERE j.cluster_id = c.cluster_id; INSERT INTO jobs VALUES (1, NULL, NULL, NULL); COMMIT;

LOG = structlog.get_logger("db")


def schema(con: sqlite3.Connection):
    with con:
        con.execute(
            """CREATE TABLE job_classads (
                   cmd TEXT, iwd TEXT, q_date INTEGER, my_type TEXT, job_prio INTEGER,
                   user_log TEXT, cluster_id INTEGER PRIMARY KEY, stream_err INTEGER,
                   stream_out INTEGER, docker_image TEXT, environment TEXT,
                   job_universe INTEGER, request_cpus INTEGER, request_disk INTEGER,
                   job_batch_name TEXT, requirements TEXT, condor_version TEXT,
                   request_memory INTEGER, transfer_input TEXT, condor_platform TEXT,
                   transfer_output TEXT, job_notification INTEGER,
                   job_submit_method INTEGER, leave_job_in_queue INTEGER,
                   docker_pull_policy TEXT, job_lease_duration INTEGER,
                   should_transfer_files TEXT, transfer_input_sizemb INTEGER,
                   entered_current_status INTEGER, transfer_output_remaps TEXT,
                   when_to_transfer_output TEXT
            ) WITHOUT ROWID"""
        )

        con.execute(
            """CREATE TABLE jobs (
                   cluster_id INTEGER PRIMARY KEY,
                   num_procs INTEGER,
                   job_name TEXT,
                   submitted_on DATE,
                   FOREIGN KEY(cluster_id) REFERENCES job_classads(cluster_id)
            ) WITHOUT ROWID
            """
        )

        con.execute(
            """CREATE TABLE procs (
                   cluster_id INTEGER PRIMARY KEY,
                   in_files TEXT,
                   out_files TEXT,
                   params TEXT
            ) WITHOUT ROWID"""
        )

        con.execute(
            "CREATE UNIQUE INDEX job_classads_cluster_id ON job_classads(cluster_id)"
        )
        con.execute("CREATE UNIQUE INDEX jobs_cluster_id ON jobs(cluster_id)")
        con.execute("CREATE UNIQUE INDEX procs_cluster_id ON procs(cluster_id)")


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


def insert_classad_job_data(con: sqlite3.Connection):
    try:
        with con:
            con.execute(
                """INSERT INTO job_classads VALUES (
                       NULL, NULL, NULL, NULL, NULL, NULL, 1, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL
                )"""
            )
            con.execute(
                """UPDATE jobs AS j
                       SET cluster_id = c.cluster_id
                       FROM job_classads AS c
                       WHERE j.cluster_id = c.cluster_id
                """
            )
            con.execute("INSERT INTO jobs VALUES (1, NULL, NULL, NULL)")
    except sqlite3.IntegrityError as e:
        LOG.exception(e)
