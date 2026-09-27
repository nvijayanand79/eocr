"""eOCR simulator console - a browser front end that lets a person drive one eOCR execution end to end:

  create execution -> upload documents to the intake bucket -> write the control file -> call the onboarding API
  -> track it with the Status API -> receive the callback -> retrieve *Response.json -> close

Every step is recorded in the batch ledger (eocr_sim.py) so the whole trace survives restarts and is shared with
the callback receiver and with scripted scenario runs. It runs inside `serve`, on its own port bound to loopback
by default, so it is never exposed through the VPC Lattice callback service; reach it with an SSM port-forward.
"""
import json
import os
import re
import secrets
import threading
import time
import traceback
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import eocr_sim as sim
import store
from eocr_sim import s3, INTAKE_BUCKET, OUTPUT_BUCKET, SIM_PREFIX, Conflict, add_event, now_iso, update_batch, load_batch

HERE = os.path.dirname(os.path.abspath(__file__))
TRACK_INTERVAL_S = int(os.environ.get("SIM_TRACK_INTERVAL_SECONDS", "15"))
MAX_UPLOAD = int(os.environ.get("SIM_MAX_UPLOAD_MB", "512")) * 1024 * 1024
CONTENT_TYPES = {".pdf": "application/pdf", ".tif": "image/tiff", ".tiff": "image/tiff", ".png": "image/png",
                 ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".json": "application/json", ".txt": "text/plain"}


# ------------------------------------------------------------------ who is using the console
#
# SIM_AUTH=oidc  the console sits behind an ALB with OIDC (company SSO / Cognito). The ALB signs the user's claims into
#                x-amzn-oidc-data (ES256); the signature is verified against the ALB's regional public key, so the
#                identity cannot be forged even by someone who reaches the port directly. No sign-in screen.
# SIM_AUTH=name  (default) a sign-in screen that asks for a name and keeps it in a cookie. It records who did what;
#                it is not authentication, which is why the console then stays on loopback behind SSM.
# SIM_AUTH=ace   ACE users sign in with their ACE user ID/e-mail and password. The ONLY thing shared with ACE is its
#                user table, read only (active users, BCrypt hash, as ACE's own login checks it). The console has its
#                own session (random token in an HttpOnly cookie, stored hashed in the simulator's schema), its own
#                sign-in log and lock-out; it never calls ACE's auth service or uses ACE tokens.

AUTH = os.environ.get("SIM_AUTH", "name").lower()
ALB_ARN = os.environ.get("SIM_ALB_ARN", "")
OIDC_KEY_URL = os.environ.get("SIM_OIDC_KEY_URL", "https://public-keys.auth.elb.{region}.amazonaws.com/{kid}")
OIDC_CLAIMS = [c.strip() for c in os.environ.get("SIM_OIDC_USER_CLAIMS", "email,preferred_username,name,sub").split(",") if c.strip()]
COOKIE = "eocr_user"
SESSION_COOKIE = "eocr_session"
SESSION_HOURS = int(os.environ.get("SIM_SESSION_HOURS", "8"))
MAX_FAILURES = 5  # per login or client address in 15 minutes
BASE = os.environ.get("SIM_BASE_PATH", "").rstrip("/")  # e.g. /eocr-sim when served behind CloudFront + ALB
_alb_keys = {}


def _b64(part):
    import base64
    part = part.rstrip("=")
    return base64.urlsafe_b64decode(part + "=" * (-len(part) % 4))


def oidc_user(token):
    """The user in an ALB x-amzn-oidc-data token, or None when it is missing, forged, from another ALB or expired."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec, utils
    from cryptography.hazmat.primitives.serialization import load_pem_public_key
    try:
        h64, p64, s64 = token.split(".")
        header = json.loads(_b64(h64))
        if header.get("alg") != "ES256" or (ALB_ARN and header.get("signer") != ALB_ARN):
            return None
        kid = header["kid"]
        if not re.fullmatch(r"[A-Za-z0-9-]{1,128}", kid):
            return None
        if kid not in _alb_keys:
            import urllib.request
            with urllib.request.urlopen(OIDC_KEY_URL.format(region=sim.REGION, kid=kid), timeout=10) as r:
                _alb_keys[kid] = load_pem_public_key(r.read())
        sig = _b64(s64)
        der = utils.encode_dss_signature(int.from_bytes(sig[:32], "big"), int.from_bytes(sig[32:], "big"))
        _alb_keys[kid].verify(der, f"{h64}.{p64}".encode(), ec.ECDSA(hashes.SHA256()))
        claims = json.loads(_b64(p64))
        if claims.get("exp") and float(claims["exp"]) < time.time():
            return None
        return next((str(claims[c]) for c in OIDC_CLAIMS if claims.get(c)), None)
    except Exception as exc:
        sim.log("OIDC token rejected", error=f"{type(exc).__name__}: {exc}")
        return None


def clean_name(value):
    return re.sub(r"[^\w .@'-]", "", str(value or ""))[:64].strip()


def client_ip(handler):
    forwarded = handler.headers.get("X-Forwarded-For") or ""
    return (forwarded.split(",")[0].strip() or handler.client_address[0])[:64]


def _cookies(handler):
    from http.cookies import SimpleCookie
    jar = SimpleCookie()
    try:
        jar.load(handler.headers.get("Cookie") or "")
    except Exception:
        pass
    return jar


def ace_sign_in(handler, login, password):
    """Check an ACE user's credentials against ACE's user table (read only) and open a console session."""
    import bcrypt
    login = str(login or "").strip()[:200]
    ip = client_ip(handler)
    db = store.get()
    if not login or not password:
        raise ValueError("enter your ACE user ID and password")
    if db.recent_failures(login, ip) >= MAX_FAILURES:
        db.record_sign_in(login, None, False, "locked out", ip)
        raise Conflict("too many failed sign-ins; wait 15 minutes")
    user = db.ace_user(login)
    ok = bool(user and user["hash"] and user["hash"].startswith("$2")
              and bcrypt.checkpw(password.encode("utf-8"), user["hash"].replace("$2y$", "$2b$", 1).encode("utf-8")))
    db.record_sign_in(login, user["userId"] if user else None, ok, None if ok else ("unknown or inactive user" if not user else "wrong password"), ip)
    if not ok:
        sim.log("console sign-in refused", login=login[:3] + "***", ip=ip)
        raise PermissionError("user ID or password is not correct")
    token = secrets.token_urlsafe(32)
    display = f"{user['name']} ({user['userId']})"
    db.create_session(store.token_hash(token), user["userId"], display, SESSION_HOURS, ip)
    handler.cookie = f"{SESSION_COOKIE}={token}; Path={BASE or '/'}; Max-Age={SESSION_HOURS * 3600}; HttpOnly; Secure; SameSite=Strict"
    sim.log("console sign-in", user=user["userId"], ip=ip)
    return display


def current_user(handler):
    if AUTH == "ace":
        jar = _cookies(handler)
        if SESSION_COOKIE not in jar:
            return None
        s = store.get().session(store.token_hash(jar[SESSION_COOKIE].value))
        return s["name"] if s else None
    if AUTH == "oidc":
        token = handler.headers.get("x-amzn-oidc-data")
        return oidc_user(token) if token else None
    from http.cookies import SimpleCookie
    jar = SimpleCookie()
    try:
        jar.load(handler.headers.get("Cookie") or "")
    except Exception:
        return None
    return clean_name(urllib.parse.unquote(jar[COOKIE].value)) if COOKIE in jar else None


# ------------------------------------------------------------------ actions (each one is a step eOCR takes)

def get(cid):
    batch, _ = load_batch(cid)
    if batch is None:
        raise KeyError(cid)
    return batch


def editable(batch):
    if batch["state"] not in ("DRAFT", "STAGED", "REJECTED"):
        raise Conflict(f"execution is {batch['state']}; documents and control file can only change before submission")


def unstage(batch):
    if batch["state"] == "STAGED":
        batch["state"] = "DRAFT"
        add_event(batch, "unstaged", "package changed after the control file was written; write the control file again")


def put_document(cid, name, content_type, body, length, origin="upload"):
    if not sim.SAFE_FILE.fullmatch(name or ""):
        raise ValueError("file name must be a plain file name (letters, digits, space, . _ - ( ))")
    batch = get(cid)
    editable(batch)
    if name == batch["controlFileName"]:
        raise ValueError("that name is reserved for the control file")
    content_type = content_type or CONTENT_TYPES.get(os.path.splitext(name)[1].lower(), "application/octet-stream")
    s3.upload_fileobj(body, INTAKE_BUCKET, batch["folder"] + name, ExtraArgs={"ContentType": content_type})

    def change(b):
        editable(b)
        b["documents"] = [d for d in b["documents"] if d["fileName"] != name] + [
            {"fileName": name, "contentType": content_type, "bytes": length, "uploadedAt": now_iso(), "origin": origin}]
        unstage(b)
        add_event(b, "uploaded", f"{name} ({length:,} bytes, {content_type}) -> s3://{INTAKE_BUCKET}/{b['folder']}{name}", origin=origin)
    return update_batch(cid, change)


def remove_document(cid, name):
    def change(b):
        editable(b)
        if name not in {d["fileName"] for d in b["documents"]}:
            raise KeyError(name)
        b["documents"] = [d for d in b["documents"] if d["fileName"] != name]
        unstage(b)
        add_event(b, "removed", f"{name} removed from the package")
    batch = update_batch(cid, change)
    s3.delete_object(Bucket=INTAKE_BUCKET, Key=batch["folder"] + name)
    return batch


def add_test_document(cid, kind, pages=None):
    """Sample documents built from the environment's test loan (s3://<config>/test-data/eocr/)."""
    import io
    if kind == "package":
        loan = json.loads(sim.testdata("loan.json"))
        pkg = sim.testdata(loan["file"])
        total = len(sim.PdfReader(io.BytesIO(pkg)).pages)
        note_first, note_last = loan["notePages"]
        count = max(note_last - note_first + 1, min(int(pages or 40), total))
        first = max(1, min(note_first, total - count + 1))
        data, name = sim.page_slice(pkg, first, count), f"{loan['loanId']}_p{first}-{first + count - 1}.pdf"
    elif kind == "locked":
        data, name = sim.password_protected_pdf(), "locked.pdf"
    elif kind == "corrupt":
        data, name = b"%PDF-1.4\n1 0 obj garbage\n", "broken.pdf"
    else:
        raise ValueError(f"unknown test document kind {kind}")
    return put_document(cid, name, "application/pdf", io.BytesIO(data), len(data), origin=f"test-data:{kind}")


def test_loan():
    try:
        loan = json.loads(sim.testdata("loan.json"))
        return {"loanId": loan["loanId"], "loanInfo": loan.get("loanInfo", {}), "file": loan.get("file"), "notePages": loan.get("notePages")}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def set_control(cid, fields):
    def change(b):
        editable(b)
        if "loanInfo" in fields:
            b["loanInfo"] = {str(k): str(v) for k, v in fields["loanInfo"].items() if k not in ("loanId", "correlationId")}
        if "extractionRequired" in fields:
            b["extractionRequired"] = bool(fields["extractionRequired"])
        if "controlOverride" in fields:
            override = fields["controlOverride"]
            if override is not None and not isinstance(override, dict):
                raise ValueError("raw control file must be a JSON object")
            b["controlOverride"] = override
        unstage(b)
        add_event(b, "control-edited", "control file " + ("replaced by hand-edited JSON" if b["controlOverride"] is not None else "fields updated"))
    return update_batch(cid, change)


def stage(cid):
    """Write the control file next to the documents (spec 3.1 / 3.2)."""
    batch = get(cid)
    editable(batch)
    control = sim.build_control_file(batch)
    s3.put_object(Bucket=INTAKE_BUCKET, Key=batch["folder"] + batch["controlFileName"], Body=json.dumps(control, indent=2).encode(), ContentType="application/json")
    present = {o["Key"][len(batch["folder"]):] for o in s3.list_objects_v2(Bucket=INTAKE_BUCKET, Prefix=batch["folder"]).get("Contents", [])}
    types = {d["fileName"]: d["contentType"] for d in batch["documents"]}
    issues = sim.control_file_issues(control, batch, {n: types.get(n) for n in present})
    listed = [d.get("fileName") for d in control.get("documents", []) if isinstance(d, dict)] if isinstance(control, dict) else []

    def change(b):
        editable(b)
        b.update(state="STAGED", controlFile=control)
        add_event(b, "staged", f"control file written to s3://{INTAKE_BUCKET}/{b['folder']}{b['controlFileName']} listing {len(listed)} documents", controlFile=control)
        b["controlIssues"] = issues
        for i in issues:
            add_event(b, "warning", f"control file (spec 3.2): {i}")
    return update_batch(cid, change)


def submit(cid, batch_path_mode="file", flaky=0, ace_url=None):
    """POST /integration/loan/onboarding (spec 4) to ace_url, or the default ACE endpoint."""
    batch = get(cid)
    ace_url = sim.check_ace_url(ace_url) if ace_url else sim.default_ace_url()
    if not ace_url:
        raise ValueError("no ACE endpoint: set a default or give one for this submission")
    if batch["state"] not in ("STAGED", "REJECTED"):
        raise Conflict(f"execution is {batch['state']}; write the control file first" if batch["state"] == "DRAFT" else f"execution is already {batch['state']}")
    batch_path = batch["folder"] if batch_path_mode == "folder" else batch["folder"] + batch["controlFileName"]
    request = {"loanId": batch["loanId"], "correlationId": batch["correlationId"], "batchPath": batch_path}
    meta = {}
    code, resp = sim.call("POST", "/integration/loan/onboarding", request, base=ace_url, meta=meta)
    job = resp.get("aceJobId") if isinstance(resp, dict) else None
    accepted = 200 <= code < 300 and bool(job)  # accepted even when the HTTP code is wrong: the job exists and must be tracked
    if accepted:
        flaky = max(0, min(int(flaky or 0), 10))
        if flaky:
            store.put_json(f"{SIM_PREFIX}flaky/{job}", {"refuse": flaky})
        sim.index_job(job, cid)
    ack_errors = []
    if 200 <= code < 300:
        if code != 202:
            ack_errors.append(f"onboarding acknowledgement: HTTP {code}, spec 4 requires 202 Accepted")
        if not (isinstance(resp, dict) and set(resp) == {"aceJobId", "status"} and
                resp.get("status") == {"code": 202, "value": "ACCEPTED", "description": "Request accepted for processing."}):
            ack_errors.append(f"onboarding acknowledgement does not match spec 4.2: {resp}")
        if not job:
            ack_errors.append("onboarding acknowledgement carries no aceJobId")
        ack_errors += sim.json_content_type(meta.get("contentType"), "onboarding acknowledgement")

    def change(b):
        b["batchPath"] = batch_path
        b["aceUrl"] = ace_url
        if accepted:
            b.update(state="SUBMITTED", aceJobId=job, flaky=flaky, submittedAt=now_iso(), submittedBy=sim.operator())
            add_event(b, "submitted", f"onboarding at {ace_url} -> HTTP {code}, aceJobId {job}" + (f"; callback endpoint will refuse the first {flaky} deliveries" if flaky else ""),
                      request=request, response=resp)
        for msg in ack_errors:
            if msg not in b["contractErrors"]:
                b["contractErrors"].append(msg)
            add_event(b, "contract-error", msg)
        if not accepted:
            b.update(state="REJECTED", submittedAt=now_iso(), submittedBy=sim.operator())
            add_event(b, "rejected", f"onboarding at {ace_url} -> HTTP {code}", request=request, response=resp)
    update_batch(cid, change)
    sim.store_submission(cid, request, code, resp)
    return get(cid)


def hitl_review(cid):
    batch = get(cid)
    code, body = sim.call("POST", "/validate/reviewValidation", {"clientLoanNumber": batch["loanId"], "adr": batch["aceJobId"]}, base=batch.get("aceUrl"))
    if code == 200 and isinstance(body, dict) and isinstance(body.get("fieldDetails"), list):
        def keep(b):  # the review is what the NOTE said; keep it for the validation view after the job ends
            b["hitl"] = {**(b.get("hitl") or {}), "loadedAt": now_iso(), "loadedBy": sim.operator(), "fields": body["fieldDetails"],
                         "totalMatches": body.get("totalMatches")}
            add_event(b, "hitl", f"HITL review loaded: {len(body['fieldDetails'])} fields, totalMatches {body.get('totalMatches')}")
        update_batch(cid, keep)
    return {"httpStatus": code, "review": body}


def hitl_decide(cid, fields):
    """Acts as the ACE HITL reviewer (not an eOCR step) so a VALIDATION_FAILED or HITL path can be closed from here."""
    batch = get(cid)
    code, body = sim.call("POST", "/validate/updateValidation", {"clientLoanNumber": batch["loanId"], "adr": batch["aceJobId"], "updatedFields": fields}, base=batch.get("aceUrl"))
    rejected = [f["fieldName"] for f in fields if not f.get("isMatched")]

    def keep(b):
        b["hitl"] = {**(b.get("hitl") or {}), "decision": {"at": now_iso(), "by": sim.operator(), "httpStatus": code,
                                                         "fields": [{"fieldName": f.get("fieldName"), "isMatched": bool(f.get("isMatched"))} for f in fields]}}
        add_event(b, "hitl", f"HITL reviewer decision sent -> HTTP {code}; mismatched: {rejected or 'none'}", response=body)
    update_batch(cid, keep)
    return {"httpStatus": code, "response": body}


def close(cid, note=""):
    """Close without (or before) a callback: reconcile against the Status API (spec 7) and retrieve the result if terminal."""
    batch = get(cid)
    if batch["state"] == "CLOSED":
        raise Conflict("execution is already closed")
    body = None
    if batch.get("aceJobId"):
        meta = {}
        code, body = sim.call("GET", f"/integration/loan/status/{batch['aceJobId']}", base=batch.get("aceUrl"), meta=meta)
        body = body if code == 200 and isinstance(body, dict) else None
    st = (body or {}).get("status") or {}
    terminal = st.get("code") in sim.TERMINAL
    result = sim.retrieve_result(batch, st.get("code"), body.get("batchPath")) if terminal else None

    def change(b):
        if b["state"] == "CLOSED":
            raise Conflict("execution was closed meanwhile (probably by its callback)")
        if body:
            sim.record_status(b, body, "close", meta.get("contentType"))
        by = "reconciliation" if terminal else "manual"
        b.update(state="CLOSED", closedAt=now_iso(), closedBy=by, closedByUser=sim.operator())
        if terminal:
            b["result"] = result
            b["outcome"] = {k: st.get(k) for k in ("code", "value", "description")} | {"batchPath": body.get("batchPath"), "failedDocuments": body.get("failedDocuments")}
            if result["key"]:
                add_event(b, "result", f"Response.json {'retrieved and matches spec 6.4/6.5' if result['ok'] else 'has contract errors'}: {result['key']}", errors=result["errors"])
        else:
            b["outcome"] = {"code": None, "value": "ABANDONED", "description": note or "closed by the operator before ACE finished",
                            "batchPath": "", "failedDocuments": []}
        if terminal:
            sim._validate(b)
        add_event(b, "closed", f"execution closed by {by}" + (f" from Status API {st.get('code')} {st.get('value')} (no callback received)" if terminal and not b.get("callback") else "")
                  + (f": {note}" if note else ""))
    update_batch(cid, change)
    sim.store_outcome(cid)
    return get(cid)


def resubmit(cid):
    """A new eOCR execution of the same package: same documents and loan data, new correlationId."""
    src = get(cid)
    new = sim.create_batch(src["loanId"], None, src["loanInfo"], src["extractionRequired"], src["controlFileName"])
    for d in src["documents"]:
        s3.copy_object(Bucket=INTAKE_BUCKET, Key=new["folder"] + d["fileName"], CopySource={"Bucket": INTAKE_BUCKET, "Key": src["folder"] + d["fileName"]},
                       ContentType=d["contentType"], MetadataDirective="REPLACE")

    def change(b):
        b["documents"] = [dict(d, uploadedAt=now_iso(), origin=f"copied from {cid}") for d in src["documents"]]
        b["resubmissionOf"] = cid
        add_event(b, "copied", f"{len(src['documents'])} documents copied from execution {cid}")
    new = update_batch(new["correlationId"], change)
    update_batch(cid, lambda b: add_event(b, "resubmitted", f"resubmitted as new execution {new['correlationId']}"))
    return new


def detail(cid):
    batch = get(cid)
    batch.pop("terminalSeenAt", None)
    batch["callbacks"] = sim.callback_records(batch["aceJobId"]) if batch.get("aceJobId") else []
    batch["controlPreview"] = sim.build_control_file(batch)
    batch["records"] = sim.list_records(cid)
    if batch["state"] != "CLOSED" or not batch.get("validation"):
        batch["validation"] = sim.validation_view(batch)  # live view while open; frozen at close
    batch["specChecklist"] = sim.spec_checklist(batch)
    batch["s3"] = {"intake": f"s3://{INTAKE_BUCKET}/{batch['folder']}", "output": f"s3://{OUTPUT_BUCKET}/{batch['aceJobId']}/" if batch.get("aceJobId") else None}
    return batch


def reports():
    objs = store.get().list(f"{SIM_PREFIX}reports/")
    out = []
    for o in sorted(objs, key=lambda o: o.key, reverse=True)[:30]:
        r = store.get_json(o.key)[0]
        out.append({"key": o.key, "finishedAt": r.get("finishedAt"), "passed": r.get("passed"),
                    "results": [{k: x.get(k) for k in ("scenario", "passed", "aceJobId", "correlationId", "minutes", "errors", "checks")} for x in r.get("results", [])]})
    return out


# ------------------------------------------------------------------ tracker: follows open executions with the Status API

def tracker():
    while True:
        try:
            keys = [o.key for o in store.get().list(sim.ACTIVE_PREFIX)]
            for key in keys:
                cid = key[len(sim.ACTIVE_PREFIX):]
                try:
                    sim.refresh_status(cid)
                except KeyError:
                    store.get().delete(key)
                except Exception as exc:
                    sim.log("tracker: status refresh failed", correlationId=cid, error=f"{type(exc).__name__}: {exc}")
        except Exception:
            sim.log("tracker failed", trace=traceback.format_exc()[-1500:])
        time.sleep(TRACK_INTERVAL_S)


# ------------------------------------------------------------------ HTTP API + page

class _Limited:
    """The request body as a file object that stops at Content-Length."""
    def __init__(self, raw, length):
        self.raw, self.left = raw, length

    def read(self, n=-1):
        n = self.left if n is None or n < 0 else min(n, self.left)
        data = self.raw.read(n) if n else b""
        self.left -= len(data)
        return data


ROUTES = []


def route(method, pattern):
    def register(fn):
        ROUTES.append((method, re.compile(pattern + "$"), fn))
        return fn
    return register


CID = r"/api/batches/(?P<cid>[A-Za-z0-9][A-Za-z0-9._-]{0,127})"


PUBLIC = {"/api/me", "/api/login", "/api/logout"}


@route("GET", r"/api/me")
def _me(h, q):
    return {"user": sim.operator(), "mode": AUTH}


@route("POST", r"/api/login")
def _login(h, q):
    if AUTH == "ace":
        body = h.json()
        return {"user": ace_sign_in(h, body.get("login"), body.get("password")), "mode": AUTH}
    if AUTH != "name":
        raise Conflict("sign-in is handled by single sign-on")
    name = clean_name(h.json().get("name"))
    if len(name) < 2:
        raise ValueError("enter your name (at least 2 characters)")
    h.cookie = f"{COOKIE}={urllib.parse.quote(name)}; Path=/; Max-Age=2592000; SameSite=Strict; HttpOnly"
    sim.log("console sign-in", user=name)
    return {"user": name, "mode": AUTH}


@route("POST", r"/api/logout")
def _logout(h, q):
    if AUTH == "ace":
        jar = _cookies(h)
        if SESSION_COOKIE in jar:
            store.get().end_session(store.token_hash(jar[SESSION_COOKIE].value))
        h.cookie = f"{SESSION_COOKIE}=; Path={BASE or '/'}; Max-Age=0; HttpOnly; Secure; SameSite=Strict"
        return {"user": None, "mode": AUTH}
    h.cookie = f"{COOKIE}=; Path=/; Max-Age=0; SameSite=Strict; HttpOnly"
    return {"user": None, "mode": AUTH}


@route("GET", r"/api/config")
def _config(h, q):
    return {"integrationUrl": sim.INTEGRATION_URL, "defaultAceUrl": sim.default_ace_url(), "aceUrlAllowed": sim.ACE_URL_ALLOWED, "intakeBucket": INTAKE_BUCKET, "outputBucket": OUTPUT_BUCKET,
            "testData": f"s3://{sim.TESTDATA_BUCKET}/{sim.TESTDATA_PREFIX}", "callbackPort": sim.PORT, "trackIntervalSeconds": TRACK_INTERVAL_S,
            "callbackGraceSeconds": sim.CALLBACK_GRACE_S}


@route("PUT", r"/api/settings")
def _settings(h, q):
    """{"aceUrl": "..."} sets the default ACE endpoint (stored in S3); an empty value goes back to ACE_URL_INTEGRATION."""
    sim.save_default_ace_url((h.json().get("aceUrl") or "").strip())
    return _config(h, q)


@route("GET", r"/api/testloan")
def _testloan(h, q):
    return test_loan()


@route("GET", r"/api/batches")
def _list(h, q):
    """{items, total, offset, limit}; state: a lifecycle state, "open" or "attention"; mine=1: created or submitted by me."""
    rows = sim.list_batches(q.get("q", ""), q.get("days") or None, limit=10 ** 9)
    state, me = q.get("state", ""), sim.operator()
    if q.get("mine") and me:
        rows = [r for r in rows if me in (r.get("createdBy"), r.get("submittedBy"))]
    if state == "open":
        rows = [r for r in rows if r["state"] not in ("CLOSED", "REJECTED")]
    elif state == "attention":
        rows = [r for r in rows if sim.needs_attention(r)]
    elif state:
        rows = [r for r in rows if r["state"] == state]
    offset, limit = max(0, int(q.get("offset") or 0)), q.get("limit", "50")
    limit = len(rows) if limit == "all" else max(1, min(int(limit), 1000))
    return {"items": rows[offset:offset + limit], "total": len(rows), "offset": offset, "limit": limit}


@route("GET", r"/api/callbacks")
def _callbacks(h, q):
    """{items, total, offset, limit, attention}; filter: accepted | refused | errors | unknown; q matches aceJobId / correlationId."""
    rows = sim.list_callbacks(10 ** 9)
    attention = sum(1 for c in rows if c["contractErrors"] or not c.get("correlationId"))
    f, text = q.get("filter", ""), q.get("q", "").strip().lower()
    rows = [c for c in rows if (not f or (f == "accepted" and c["answeredWith"] == 200) or (f == "refused" and c["answeredWith"] != 200)
                                or (f == "errors" and c["contractErrors"]) or (f == "unknown" and not c.get("correlationId")))
            and (not text or text in f"{c['aceJobId']} {c.get('correlationId') or ''}".lower())]
    offset, limit = max(0, int(q.get("offset") or 0)), max(1, min(int(q.get("limit") or 100), 1000))
    return {"items": rows[offset:offset + limit], "total": len(rows), "offset": offset, "limit": limit, "attention": attention}


@route("POST", r"/api/batches")
def _create(h, q):
    b = h.json()
    return sim.create_batch(str(b.get("loanId") or "").strip(), (b.get("correlationId") or "").strip() or None, b.get("loanInfo") or {},
                            b.get("extractionRequired", True), (b.get("controlFileName") or "controlfile.json").strip())


@route("GET", CID)
def _detail(h, q, cid):
    return detail(cid)


@route("PUT", CID + r"/files/(?P<name>[^/]+)")
def _upload(h, q, cid, name):
    length = int(h.headers.get("Content-Length") or 0)
    if length <= 0 or length > MAX_UPLOAD:
        raise ValueError(f"upload must be 1 byte to {MAX_UPLOAD // 1048576} MB")
    return put_document(cid, urllib.parse.unquote(name), q.get("contentType") or h.headers.get("Content-Type"), _Limited(h.rfile, length), length)


@route("DELETE", CID + r"/files/(?P<name>[^/]+)")
def _remove(h, q, cid, name):
    return remove_document(cid, urllib.parse.unquote(name))


@route("POST", CID + r"/testdata")
def _testdata(h, q, cid):
    b = h.json()
    return add_test_document(cid, b.get("kind"), b.get("pages"))


@route("PUT", CID + r"/control")
def _control(h, q, cid):
    return set_control(cid, h.json())


@route("POST", CID + r"/stage")
def _stage(h, q, cid):
    return stage(cid)


@route("POST", CID + r"/submit")
def _submit(h, q, cid):
    b = h.json()
    return submit(cid, b.get("batchPathMode", "file"), b.get("flaky", 0), (b.get("aceUrl") or "").strip() or None)


@route("POST", CID + r"/refresh")
def _refresh(h, q, cid):
    return sim.refresh_status(cid, "operator")


@route("GET", CID + r"/hitl")
def _hitl_get(h, q, cid):
    return hitl_review(cid)


@route("POST", CID + r"/hitl")
def _hitl_post(h, q, cid):
    return hitl_decide(cid, h.json().get("fields") or [])


@route("POST", CID + r"/close")
def _close(h, q, cid):
    return close(cid, str(h.json().get("note") or ""))


@route("POST", CID + r"/resubmit")
def _resubmit(h, q, cid):
    return resubmit(cid)


@route("GET", CID + r"/response")
def _response(h, q, cid):
    batch = get(cid)
    key = (batch.get("outcome") or {}).get("batchPath") or (batch.get("status") or {}).get("batchPath")
    if not key:
        raise KeyError("no Response.json for this execution")
    try:
        return json.loads(s3.get_object(Bucket=OUTPUT_BUCKET, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        if not batch.get("responseCopy"):
            raise KeyError(f"{key} is gone from the output bucket and no copy was stored") from None
        return sim.read_record(cid, batch["responseCopy"].rsplit("/", 1)[-1])


@route("GET", CID + r"/records")
def _records(h, q, cid):
    return sim.list_records(cid)


@route("GET", CID + r"/records/(?P<name>[^/]+)")
def _record(h, q, cid, name):
    return sim.read_record(cid, urllib.parse.unquote(name))


class Download:
    def __init__(self, data, content_type, filename):
        self.data, self.content_type, self.filename = data, content_type, filename


def to_csv(rows, columns, filename):
    import csv
    import io
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([c for c, _ in columns])
    for r in rows:
        w.writerow(["" if (v := get_(r)) is None else v for _, get_ in columns])
    return Download(buf.getvalue().encode("utf-8-sig"), "text/csv; charset=utf-8", filename)


def _minutes(a, b):
    try:
        return round((sim.datetime.fromisoformat(b.replace("Z", "+00:00")) - sim.datetime.fromisoformat(a.replace("Z", "+00:00"))).total_seconds() / 60, 1)
    except (AttributeError, TypeError, ValueError):
        return None


@route("GET", r"/api/export/executions\.csv")
def _export_executions(h, q):
    rows = sim.list_batches(q.get("q", ""), q.get("days") or None, limit=100000)
    o = lambda r, k: (r.get("outcome") or {}).get(k)  # noqa: E731
    return to_csv(rows, [
        ("correlationId", lambda r: r["correlationId"]), ("loanId", lambda r: r["loanId"]), ("aceJobId", lambda r: r["aceJobId"]),
        ("state", lambda r: r["state"]), ("outcomeCode", lambda r: o(r, "code")), ("outcome", lambda r: o(r, "value")),
        ("description", lambda r: o(r, "description")), ("failedDocuments", lambda r: "; ".join(f"{f.get('documentName')}: {f.get('reason')}" for f in o(r, "failedDocuments") or [])),
        ("resultFile", lambda r: o(r, "batchPath")), ("aceStage", lambda r: (r.get("workflow") or {}).get("stage")), ("aceState", lambda r: (r.get("workflow") or {}).get("state")),
        ("createdAt", lambda r: r["createdAt"]), ("createdBy", lambda r: r.get("createdBy")), ("submittedAt", lambda r: r.get("submittedAt")),
        ("submittedBy", lambda r: r.get("submittedBy")), ("callbackAt", lambda r: r.get("callbackAt")), ("callbackAttempts", lambda r: r.get("callbackAttempts")),
        ("minutesToCallback", lambda r: _minutes(r.get("submittedAt"), r.get("callbackAt"))), ("closedAt", lambda r: r.get("closedAt")),
        ("closedBy", lambda r: r.get("closedBy")), ("closedByUser", lambda r: r.get("closedByUser")), ("contractErrors", lambda r: r.get("contractErrors")),
        ("aceEndpoint", lambda r: r.get("aceUrl")), ("source", lambda r: r.get("source")), ("documents", lambda r: r.get("documents")),
    ], f"eocr-executions-{sim.now_iso()[:10]}.csv")


@route("GET", r"/api/export/callbacks\.csv")
def _export_callbacks(h, q):
    rows = sim.list_callbacks(100000)
    p = lambda r: r.get("payload") or {}  # noqa: E731
    s_ = lambda r: p(r).get("status") if isinstance(p(r).get("status"), dict) else {}  # noqa: E731
    return to_csv(rows, [
        ("receivedAt", lambda r: r["receivedAt"]), ("aceJobId", lambda r: r["aceJobId"]), ("correlationId", lambda r: r.get("correlationId")),
        ("attempt", lambda r: r["attempt"]), ("answeredWith", lambda r: r["answeredWith"]), ("statusCode", lambda r: s_(r).get("code")),
        ("status", lambda r: s_(r).get("value")), ("description", lambda r: s_(r).get("description")), ("batchPath", lambda r: p(r).get("batchPath")),
        ("failedDocuments", lambda r: "; ".join(f"{f.get('documentName')}: {f.get('reason')}" for f in p(r).get("failedDocuments") or [] if isinstance(f, dict))),
        ("idempotencyKey", lambda r: r.get("idempotencyKey")), ("callerPrincipal", lambda r: r.get("callerPrincipal")),
        ("contractErrors", lambda r: " | ".join(r.get("contractErrors") or [])),
    ], f"eocr-callbacks-{sim.now_iso()[:10]}.csv")


@route("GET", r"/api/reports")
def _reports(h, q):
    return reports()


class ConsoleHandler(BaseHTTPRequestHandler):
    server_version = "eocr-sim-console"

    def log_message(self, *_):
        pass

    def json(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        body = json.loads(raw) if raw else {}
        if not isinstance(body, dict):
            raise ValueError("request body must be a JSON object")
        return body

    def _send(self, code, data, content_type="application/json", filename=None):
        if not isinstance(data, bytes):
            data = json.dumps(data, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "same-origin")
        self.send_header("Strict-Transport-Security", "max-age=31536000")
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        if getattr(self, "cookie", None):
            self.send_header("Set-Cookie", self.cookie)
        self.end_headers()
        self.wfile.write(data)

    def _dispatch(self, method):
        url = urllib.parse.urlsplit(self.path)
        if BASE:
            if url.path == BASE:
                self.send_response(301)
                self.send_header("Location", BASE + "/")
                self.end_headers()
                return
            if not url.path.startswith(BASE + "/"):
                return self._send(404, {"error": "not found"})
            url = url._replace(path=url.path[len(BASE):])
        if method == "GET" and url.path == "/health":
            return self._send(200, {"status": "UP"})
        if method == "GET" and url.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "console.html"), "rb") as f:
                page = f.read().replace(b"<head>", f'<head><meta name="eocr-base" content="{BASE}">'.encode(), 1)
            return self._send(200, page, "text/html; charset=utf-8")
        q = {k: v[-1] for k, v in urllib.parse.parse_qs(url.query).items()}
        for m, pattern, fn in ROUTES:
            match = pattern.match(url.path)
            if m == method and match:
                sim.OPERATOR.name = current_user(self)
                self.cookie = None
                if not sim.OPERATOR.name and url.path not in PUBLIC:
                    return self._send(401, {"error": "sign in first", "mode": AUTH})
                try:
                    result = fn(self, q, **match.groupdict())
                    if isinstance(result, Download):
                        return self._send(200, result.data, result.content_type, result.filename)
                    return self._send(200, result)
                except KeyError as exc:
                    return self._send(404, {"error": f"not found: {exc.args[0] if exc.args else ''}"})
                except Conflict as exc:
                    return self._send(409, {"error": str(exc)})
                except PermissionError as exc:
                    return self._send(403, {"error": str(exc), "mode": AUTH})
                except (ValueError, TypeError) as exc:
                    return self._send(400, {"error": str(exc)})
                except Exception as exc:
                    sim.log("console error", path=url.path, trace=traceback.format_exc()[-1500:])
                    return self._send(500, {"error": f"{type(exc).__name__}: {exc}"})
        self._send(404, {"error": "not found"})

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")

    def do_DELETE(self):
        self._dispatch("DELETE")


def start(host, port):
    threading.Thread(target=tracker, name="tracker", daemon=True).start()
    server = ThreadingHTTPServer((host, port), ConsoleHandler)
    threading.Thread(target=server.serve_forever, name="console", daemon=True).start()
    sim.log("eOCR simulator console listening", host=host, port=port)
    return server
