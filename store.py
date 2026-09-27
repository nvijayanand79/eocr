"""Where the simulator keeps its own records (executions, callbacks, submission/outcome records, settings,
sign-in sessions, scenario reports).

SIM_STORE=db (deployed): the environment's Aurora cluster, in the simulator's OWN schema (SIM_DB_SCHEMA,
default "eocrsim"), with its own IAM login (ACE_DB_USER, a token from the task role - no password). That login
is isolated from ACE: it owns only this schema and can read only the sign-in columns of common."user"
(see newrez-ace-infra db/migrate.py, ACE_ISOLATED_DB_USERS). ACE never reads these tables.

SIM_STORE=s3 (local development with moto): the same records as JSON objects under s3://<intake>/eocr-sim/.

Both expose the small object API the simulator uses - get / put (with If-Match / If-None-Match semantics) /
delete / list(prefix) - so the ledger logic is identical in both. The loan documents themselves always stay
in S3: that is the eOCR contract (spec 3.1).
"""
import hashlib
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3

MODE = os.environ.get("SIM_STORE", "s3").lower()
REGION = os.environ.get("AWS_REGION", "us-east-1")
SCHEMA = os.environ.get("SIM_DB_SCHEMA", "eocrsim")
CA = os.environ.get("SIM_DB_CA", "/app/rds-global-bundle.pem")


class PreconditionFailed(Exception):
    """A conditional write lost: the object changed (If-Match) or already exists (If-None-Match)."""


class Obj:
    __slots__ = ("key", "etag", "modified", "size")

    def __init__(self, key, etag, modified, size=0):
        self.key, self.etag, self.modified, self.size = key, etag, modified, size


# ------------------------------------------------------------------ Aurora (deployed)

class DbStore:
    """One table, eocrsim.object(key, body, etag, content_type, created_at, modified_at), plus readable views
    (eocrsim.executions, eocrsim.callbacks, eocrsim.sessions) over it for people who query the database."""

    DDL = """
    CREATE TABLE IF NOT EXISTS {s}.object (
        key           TEXT PRIMARY KEY,
        body          BYTEA NOT NULL,
        etag          TEXT NOT NULL,
        content_type  TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        modified_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS object_key_prefix ON {s}.object (key text_pattern_ops);
    CREATE INDEX IF NOT EXISTS object_modified ON {s}.object (modified_at DESC);
    CREATE TABLE IF NOT EXISTS {s}.session (
        token_hash  TEXT PRIMARY KEY,
        user_id     TEXT NOT NULL,
        user_name   TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        expires_at  TIMESTAMPTZ NOT NULL,
        client_ip   TEXT
    );
    CREATE TABLE IF NOT EXISTS {s}.sign_in (
        id          BIGSERIAL PRIMARY KEY,
        at          TIMESTAMPTZ NOT NULL DEFAULT now(),
        login       TEXT NOT NULL,
        user_id     TEXT,
        ok          BOOLEAN NOT NULL,
        reason      TEXT,
        client_ip   TEXT
    );
    CREATE OR REPLACE VIEW {s}.executions AS
        SELECT convert_from(body, 'UTF8')::jsonb ->> 'correlationId' AS correlation_id,
               convert_from(body, 'UTF8')::jsonb ->> 'loanId'        AS loan_id,
               convert_from(body, 'UTF8')::jsonb ->> 'aceJobId'      AS ace_job_id,
               convert_from(body, 'UTF8')::jsonb ->> 'state'         AS state,
               convert_from(body, 'UTF8')::jsonb -> 'status' -> 'status' ->> 'value' AS ace_status,
               convert_from(body, 'UTF8')::jsonb ->> 'outcome'       AS outcome,
               convert_from(body, 'UTF8')::jsonb ->> 'createdBy'     AS created_by,
               convert_from(body, 'UTF8')::jsonb ->> 'submittedBy'   AS submitted_by,
               convert_from(body, 'UTF8')::jsonb ->> 'closedByUser'  AS closed_by,
               created_at, modified_at,
               convert_from(body, 'UTF8')::jsonb AS execution
          FROM {s}.object WHERE key LIKE 'eocr-sim/batches/%';
    CREATE OR REPLACE VIEW {s}.callbacks AS
        SELECT split_part(key, '/', 3) AS ace_job_id,
               (convert_from(body, 'UTF8')::jsonb ->> 'attempt')::int AS attempt,
               (convert_from(body, 'UTF8')::jsonb ->> 'answeredWith')::int AS answered_with,
               convert_from(body, 'UTF8')::jsonb -> 'payload' -> 'status' ->> 'value' AS status_value,
               convert_from(body, 'UTF8')::jsonb ->> 'callerPrincipal' AS caller_principal,
               created_at AS received_at,
               convert_from(body, 'UTF8')::jsonb AS record
          FROM {s}.object WHERE key LIKE 'eocr-sim/callbacks/%';
    """

    def __init__(self):
        self.host = os.environ["ACE_DB_HOST"]
        self.port = int(os.environ.get("ACE_DB_PORT", "5432"))
        self.db = os.environ.get("ACE_DB_NAME", "ace")
        self.user = os.environ["ACE_DB_USER"]
        self._local = threading.local()
        self._rds = boto3.client("rds", region_name=REGION)
        with self.conn() as c:
            c.execute(self.DDL.format(s=SCHEMA))

    def _connect(self):
        import psycopg
        token = self._rds.generate_db_auth_token(DBHostname=self.host, Port=self.port, DBUsername=self.user, Region=REGION)
        c = psycopg.connect(host=self.host, port=self.port, dbname=self.db, user=self.user, password=token,
                            sslmode="verify-full", sslrootcert=CA, connect_timeout=15, application_name="eocr-simulator",
                            autocommit=True)
        c.execute(f"SET search_path = {SCHEMA}")
        return c

    def conn(self):
        """A per-thread connection (IAM tokens only matter when connecting); reopened when it has broken."""
        c = getattr(self._local, "c", None)
        if c is None or c.closed or c.broken:
            c = self._local.c = self._connect()
        return _Borrowed(c)

    def _run(self, sql, params=(), fetch=None):
        for attempt in range(2):
            try:
                with self.conn() as c:
                    cur = c.execute(sql, params)
                    if fetch == "one":
                        return cur.fetchone()
                    if fetch == "all":
                        return cur.fetchall()
                    return cur.rowcount
            except Exception as exc:
                import psycopg
                if attempt == 0 and isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
                    self._local.c = None
                    continue
                raise

    def get(self, key):
        row = self._run("SELECT body, etag, modified_at FROM object WHERE key = %s", (key,), "one")
        if not row:
            raise KeyError(key)
        return bytes(row[0]), row[1]

    def put(self, key, body, content_type="application/json", if_match=None, if_none_match=False):
        etag = uuid.uuid4().hex
        if if_none_match:
            n = self._run("INSERT INTO object (key, body, etag, content_type) VALUES (%s, %s, %s, %s) ON CONFLICT (key) DO NOTHING",
                          (key, body, etag, content_type))
        elif if_match:
            n = self._run("UPDATE object SET body = %s, etag = %s, content_type = %s, modified_at = now() WHERE key = %s AND etag = %s",
                          (body, etag, content_type, key, if_match))
        else:
            n = self._run("""INSERT INTO object (key, body, etag, content_type) VALUES (%s, %s, %s, %s)
                             ON CONFLICT (key) DO UPDATE SET body = EXCLUDED.body, etag = EXCLUDED.etag,
                             content_type = EXCLUDED.content_type, modified_at = now()""", (key, body, etag, content_type))
        if n == 0:
            raise PreconditionFailed(key)
        return etag

    def delete(self, key):
        self._run("DELETE FROM object WHERE key = %s", (key,))

    def list(self, prefix):
        rows = self._run("SELECT key, etag, modified_at, octet_length(body) FROM object WHERE key LIKE %s ORDER BY key",
                         (prefix.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%",), "all")
        return [Obj(k, e, m, s) for k, e, m, s in rows]

    # ---- sign-in (the only thing shared with ACE: its user table, read only)

    def ace_user(self, login):
        """The ACE user this login names, as ACE's own login query finds it: user_id or email, status 'Active'."""
        row = self._run("""SELECT user_id, email_id, password, first_name, last_name FROM common."user"
                            WHERE (user_id = %s OR email_id = %s) AND status = 'Active' LIMIT 1""", (login, login), "one")
        return None if not row else {"userId": row[0], "email": row[1], "hash": row[2], "name": " ".join(x for x in (row[3], row[4]) if x) or row[0]}

    def record_sign_in(self, login, user_id, ok, reason, ip):
        self._run("INSERT INTO sign_in (login, user_id, ok, reason, client_ip) VALUES (%s, %s, %s, %s, %s)", (login[:200], user_id, ok, reason, ip))

    def recent_failures(self, login, ip, minutes=15):
        row = self._run("""SELECT count(*) FROM sign_in WHERE NOT ok AND at > now() - make_interval(mins => %s)
                            AND (login = %s OR client_ip = %s)""", (minutes, login[:200], ip), "one")
        return row[0]

    def create_session(self, token_hash, user_id, user_name, hours, ip):
        self._run("INSERT INTO session (token_hash, user_id, user_name, expires_at, client_ip) VALUES (%s, %s, %s, now() + make_interval(hours => %s), %s)",
                  (token_hash, user_id, user_name, hours, ip))
        self._run("DELETE FROM session WHERE expires_at < now() - interval '1 day'")

    def session(self, token_hash):
        row = self._run("SELECT user_id, user_name FROM session WHERE token_hash = %s AND expires_at > now()", (token_hash,), "one")
        return None if not row else {"userId": row[0], "name": row[1]}

    def end_session(self, token_hash):
        self._run("DELETE FROM session WHERE token_hash = %s", (token_hash,))


class _Borrowed:
    def __init__(self, c):
        self.c = c

    def __enter__(self):
        return self.c

    def __exit__(self, *exc):
        return False


# ------------------------------------------------------------------ S3 (local development)

class S3Store:
    def __init__(self):
        self.s3 = boto3.client("s3", region_name=REGION, **({"endpoint_url": os.environ["S3_ENDPOINT_URL"]} if os.environ.get("S3_ENDPOINT_URL") else {}))
        self.bucket = os.environ.get("ACE_BUCKET_INTAKE", "")
        self._sessions = {}

    def get(self, key):
        try:
            o = self.s3.get_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.NoSuchKey:
            raise KeyError(key)
        return o["Body"].read(), o["ETag"]

    def put(self, key, body, content_type="application/json", if_match=None, if_none_match=False):
        extra = {"IfMatch": if_match} if if_match else {"IfNoneMatch": "*"} if if_none_match else {}
        try:
            return self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type, **extra)["ETag"]
        except Exception as exc:
            if getattr(exc, "response", {}).get("Error", {}).get("Code") in ("PreconditionFailed", "ConditionalRequestConflict"):
                raise PreconditionFailed(key) from exc
            raise

    def delete(self, key):
        self.s3.delete_object(Bucket=self.bucket, Key=key)

    def list(self, prefix):
        out = []
        for page in self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix):
            out += [Obj(o["Key"], o["ETag"], o["LastModified"], o["Size"]) for o in page.get("Contents", [])]
        return out

    # local development has no ACE user table: sign-in by name only (SIM_AUTH=name)
    def ace_user(self, login):
        return None

    def record_sign_in(self, *a):
        pass

    def recent_failures(self, *a, **k):
        return 0

    def create_session(self, token_hash, user_id, user_name, hours, ip):
        self._sessions[token_hash] = ({"userId": user_id, "name": user_name}, time.time() + hours * 3600)

    def session(self, token_hash):
        s = self._sessions.get(token_hash)
        return s[0] if s and s[1] > time.time() else None

    def end_session(self, token_hash):
        self._sessions.pop(token_hash, None)


_store = None
_lock = threading.Lock()


def get():
    global _store
    with _lock:
        if _store is None:
            _store = DbStore() if MODE == "db" else S3Store()
    return _store


# helpers used across the simulator

def get_json(key):
    body, etag = get().get(key)
    return json.loads(body), etag


def put_json(key, doc, **cond):
    return get().put(key, json.dumps(doc, indent=2, default=str).encode(), "application/json", **cond)


def token_hash(token):
    return hashlib.sha256(token.encode()).hexdigest()


def utcnow():
    return datetime.now(timezone.utc)
