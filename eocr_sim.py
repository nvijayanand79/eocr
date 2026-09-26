"""eOCR simulator - plays the eOCR partner system against the ACE integration service.

It follows the ACE to eOCR Integration Specification v1.1 exactly and is a TEST HARNESS, not product.

  serve   eOCR's callback endpoint (spec section 5). Every callback is checked against the contract and
          recorded in S3. A scenario can ask for the first N deliveries of its job to be refused with 503,
          which proves ACE's callback retry (an intermittent eOCR outage).
  run     the scenario driver (spec sections 3 and 4): stages each loan package in the intake bucket
          exactly as eOCR does (loanId=<id>/correlationId=<execution>/<control>.json + documents), calls
          the onboarding API, follows the Status API, waits for the callback and verifies *Response.json.

Everything is IAM: the task role signs the onboarding/status calls (SigV4, VPC Lattice) and reads/writes
S3. ACE signs its callbacks to this service the same way. No keys, tokens or passwords exist.
"""
import argparse
import io
import json
import os
import re
import random
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from pypdf import PdfReader, PdfWriter

REGION = os.environ.get("AWS_REGION", "us-east-1")
INTEGRATION_URL = os.environ.get("ACE_URL_INTEGRATION", "").rstrip("/")
INTAKE_BUCKET = os.environ.get("ACE_BUCKET_INTAKE", "")
OUTPUT_BUCKET = os.environ.get("ACE_BUCKET_OUTPUT", "")
TESTDATA_BUCKET = os.environ.get("ACE_BUCKET_CONFIG", "")
TESTDATA_PREFIX = os.environ.get("SIM_TESTDATA_PREFIX", "test-data/eocr/")
SIM_PREFIX = "eocr-sim/"  # simulator bookkeeping inside the intake bucket (eOCR's own area)
PORT = int(os.environ.get("PORT", "8080"))

STATUS = {0: "COMPLETED", 1000: "PRECHECK_FAILED", 2000: "VALIDATION_FAILED", 3000: "PROCESSING_FAILED",
          4000: "IN_PROGRESS", 202: "ACCEPTED", 4040: "JOB_NOT_FOUND"}
TERMINAL = {0, 1000, 2000, 3000}
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

s3 = boto3.client("s3", region_name=REGION)
_session = boto3.Session(region_name=REGION)


def log(msg, **kv):
    print(json.dumps({"ts": datetime.now(timezone.utc).strftime("%H:%M:%S"), "msg": msg, **kv}, default=str), flush=True)


# ------------------------------------------------------------------ contract checks (spec sections 5-7)

def check_status_object(status, where):
    errs = []
    if not isinstance(status, dict) or set(status) != {"code", "value", "description"}:
        return [f"{where}: status must be exactly {{code, value, description}}: {status}"]
    if not isinstance(status["code"], int):
        errs.append(f"{where}: status.code must be a number, got {type(status['code']).__name__}")
    elif STATUS.get(status["code"]) != status["value"]:
        errs.append(f"{where}: status.code {status['code']} does not match status.value {status['value']}")
    if not str(status.get("description") or "").strip():
        errs.append(f"{where}: status.description is empty")
    return errs


def check_output_rules(code, job_id, batch_path, failed, where):
    errs = []
    if code in (0, 2000):
        if not re.fullmatch(re.escape(job_id) + r"/[^/]+\.json", batch_path or ""):
            errs.append(f"{where}: {STATUS[code]} needs batchPath '<aceJobId>/<file>.json', got '{batch_path}'")
    elif batch_path != "":
        errs.append(f"{where}: {STATUS.get(code)} needs an empty batchPath, got '{batch_path}'")
    if not isinstance(failed, list):
        return errs + [f"{where}: failedDocuments must be an array"]
    if code == 1000:
        if not failed:
            errs.append(f"{where}: PRECHECK_FAILED needs failedDocuments")
        for item in failed:
            if not isinstance(item, dict) or set(item) != {"documentName", "reason"} or not all(item.values()):
                errs.append(f"{where}: failedDocuments item must be {{documentName, reason}}: {item}")
    elif failed:
        errs.append(f"{where}: {STATUS.get(code)} needs an empty failedDocuments array")
    return errs


def check_callback(payload):
    """Spec 5.1 / 6.x: flat, exact field set, terminal code, output rules, ISO-8601 UTC timestamp."""
    errs = []
    expected = {"aceJobId", "status", "batchPath", "failedDocuments", "timestamp"}
    if set(payload) != expected:
        errs.append(f"callback fields {sorted(payload)} != {sorted(expected)}")
    status = payload.get("status", {})
    errs += check_status_object(status, "callback")
    code = status.get("code") if isinstance(status, dict) else None
    if code not in TERMINAL:
        errs.append(f"callback: status.code {code} is not terminal")
    errs += check_output_rules(code, payload.get("aceJobId", ""), payload.get("batchPath"), payload.get("failedDocuments"), "callback")
    if not TIMESTAMP.match(str(payload.get("timestamp", ""))):
        errs.append(f"callback: timestamp '{payload.get('timestamp')}' is not ISO-8601 UTC (yyyy-MM-ddTHH:mm:ssZ)")
    return errs


def check_status_response(body, job_id):
    """Spec 7.3."""
    errs = []
    expected = {"aceJobId", "workflow", "status", "batchPath", "failedDocuments", "timestamp"}
    if set(body) != expected:
        errs.append(f"status API fields {sorted(body)} != {sorted(expected)}")
    if body.get("aceJobId") != job_id:
        errs.append(f"status API aceJobId {body.get('aceJobId')} != {job_id}")
    errs += check_status_object(body.get("status"), "status API")
    code = (body.get("status") or {}).get("code")
    errs += check_output_rules(code, job_id, body.get("batchPath"), body.get("failedDocuments"), "status API")
    if code != 4040 and not {"stage", "state"} <= set(body.get("workflow") or {}):
        errs.append(f"status API: workflow must have stage and state: {body.get('workflow')}")
    if not TIMESTAMP.match(str(body.get("timestamp", ""))):
        errs.append(f"status API: timestamp '{body.get('timestamp')}' is not ISO-8601 UTC")
    return errs


def check_response_file(doc, job_id, extraction_required, file_names, expect_validation):
    """Spec 6.4 / 6.5."""
    errs = []
    order = ["aceJobId", "extractionRequired", "validationStatus", "Documents", "fileName", "fileSize"]
    if list(doc) != order:
        errs.append(f"Response.json top-level keys {list(doc)} != {order}")
    if doc.get("aceJobId") != job_id:
        errs.append("Response.json aceJobId mismatch")
    if doc.get("extractionRequired") != ("true" if extraction_required else "false"):
        errs.append(f"Response.json extractionRequired {doc.get('extractionRequired')!r} != {extraction_required}")
    if doc.get("validationStatus") != expect_validation:
        errs.append(f"Response.json validationStatus {doc.get('validationStatus')} != {expect_validation}")
    docs = doc.get("Documents")
    if not isinstance(docs, dict) or not docs:
        return errs + ["Response.json Documents must be a non-empty object"]
    fields = ["fileName", "docTypeId", "docTypeName", "pageRange", "confidencePercentage", "duplicatePagesOf", "extraction"]
    extracted = 0
    for doc_type, items in docs.items():
        if not isinstance(items, list) or not items:
            errs.append(f"Documents['{doc_type}'] must be a non-empty array")
            continue
        for item in items:
            if list(item) != fields:
                errs.append(f"Documents['{doc_type}'] item keys {list(item)} != {fields}")
                continue
            if item["fileName"] not in file_names:
                errs.append(f"Documents['{doc_type}'] fileName '{item['fileName']}' is not a submitted document")
            if not re.fullmatch(r"\d+(\s*-\s*\d+)?(\s*,\s*\d+(\s*-\s*\d+)?)*", item["pageRange"] or ""):
                errs.append(f"Documents['{doc_type}'] pageRange '{item['pageRange']}' is malformed")
            if not isinstance(item["confidencePercentage"], dict):
                errs.append(f"Documents['{doc_type}'] confidencePercentage must be an object")
            if not isinstance(item["extraction"], dict):
                errs.append(f"Documents['{doc_type}'] extraction must be an object")
                continue
            for name, field in item["extraction"].items():
                extracted += 1
                if not isinstance(field, dict) or list(field) != ["Value", "cr"]:
                    errs.append(f"Documents['{doc_type}'].extraction['{name}'] must be {{Value, cr}}: {field}")
    if (not extraction_required or expect_validation == "FAILED") and extracted:
        errs.append(f"Response.json has {extracted} extracted fields although extraction must be empty")
    if extraction_required and expect_validation != "FAILED" and not extracted:
        errs.append("Response.json has no extracted fields although extraction was required")
    if not str(doc.get("fileSize", "")).isdigit():
        errs.append(f"Response.json fileSize {doc.get('fileSize')!r} is not a byte count")
    return errs


# ------------------------------------------------------------------ callback receiver (serve)

class CallbackHandler(BaseHTTPRequestHandler):
    server_version = "eocr-sim"

    def log_message(self, *_):
        pass

    def _reply(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/eocr/health"):
            return self._reply(200, {"status": "UP"})
        self._reply(404, {"error": "not found"})

    def do_POST(self):
        if not self.path.startswith("/eocr/callback"):
            return self._reply(404, {"error": "not found"})
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        caller = self.headers.get("x-amzn-lattice-identity", "")
        principal = re.search(r"Principal=([^;]+)", caller)
        try:
            payload = json.loads(raw)
        except ValueError:
            return self._reply(400, {"error": "body is not JSON"})
        job_id = str(payload.get("aceJobId") or "unknown")
        if not re.fullmatch(r"[A-Za-z0-9-]{1,64}", job_id):
            return self._reply(400, {"error": "invalid aceJobId"})
        base = f"{SIM_PREFIX}callbacks/{job_id}/"
        attempts = len(s3.list_objects_v2(Bucket=INTAKE_BUCKET, Prefix=base).get("Contents", [])) + 1
        refuse = 0
        try:
            refuse = int(json.loads(s3.get_object(Bucket=INTAKE_BUCKET, Key=f"{SIM_PREFIX}flaky/{job_id}")["Body"].read())["refuse"])
        except s3.exceptions.NoSuchKey:
            pass
        answered = 503 if attempts <= refuse else 200
        record = {
            "receivedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "attempt": attempts,
            "answeredWith": answered,
            "callerPrincipal": principal.group(1) if principal else None,
            "idempotencyKey": self.headers.get("Idempotency-Key"),
            "contractErrors": check_callback(payload),
            "payload": payload,
        }
        s3.put_object(Bucket=INTAKE_BUCKET, Key=f"{base}{attempts:03d}.json", Body=json.dumps(record, indent=2).encode(), ContentType="application/json")
        log("callback received", aceJobId=job_id, attempt=attempts, answered=answered,
            code=(payload.get("status") or {}).get("code"), contractErrors=len(record["contractErrors"]), caller=record["callerPrincipal"])
        if answered == 503:
            return self._reply(503, {"error": "simulated intermittent eOCR outage"})
        self._reply(200, {"received": True})


def serve():
    log("eOCR simulator callback receiver listening", port=PORT, bucket=INTAKE_BUCKET)
    ThreadingHTTPServer(("0.0.0.0", PORT), CallbackHandler).serve_forever()


# ------------------------------------------------------------------ ACE API client (SigV4 over VPC Lattice)

def call(method, path, body=None, expect=None):
    url = INTEGRATION_URL + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json", "x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = AWSRequest(method=method, url=url, data=data, headers=headers)
    SigV4Auth(_session.get_credentials().get_frozen_credentials(), "vpc-lattice-svcs", REGION).add_auth(req)
    http = urllib.request.Request(url, data=data, method=method, headers=dict(req.headers.items()))
    try:
        with urllib.request.urlopen(http, timeout=60) as r:
            code, text = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        code, text = e.code, e.read().decode(errors="replace")
    try:
        parsed = json.loads(text) if text else None
    except ValueError:
        parsed = text
    if expect is not None and code != expect:
        raise AssertionError(f"{method} {path} -> HTTP {code} (expected {expect}): {text[:400]}")
    return code, parsed


# ------------------------------------------------------------------ test documents

_cache = {}


def testdata(name):
    if name not in _cache:
        _cache[name] = s3.get_object(Bucket=TESTDATA_BUCKET, Key=TESTDATA_PREFIX + name)["Body"].read()
    return _cache[name]


def page_slice(data, first, count):
    """Pages first..first+count-1 (1-based) of a PDF, as new PDF bytes."""
    reader = PdfReader(io.BytesIO(data))
    w = PdfWriter()
    for p in range(first - 1, first - 1 + count):
        w.add_page(reader.pages[p])
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


def split_pdf(data, parts):
    reader = PdfReader(io.BytesIO(data))
    n = len(reader.pages)
    bounds = [round(i * n / parts) for i in range(parts + 1)]
    out = []
    for i in range(parts):
        w = PdfWriter()
        for p in range(bounds[i], bounds[i + 1]):
            w.add_page(reader.pages[p])
        buf = io.BytesIO()
        w.write(buf)
        out.append((buf.getvalue(), bounds[i + 1] - bounds[i]))
    return out


def password_protected_pdf():
    w = PdfWriter()
    w.add_blank_page(width=612, height=792)
    w.encrypt(user_password="eocr-sim-locked", owner_password="eocr-sim-owner")
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


def small_text_pdf(source, pages=2):
    reader = PdfReader(io.BytesIO(source))
    w = PdfWriter()
    for p in range(pages):
        w.add_page(reader.pages[p])
    buf = io.BytesIO()
    w.write(buf)
    return buf.getvalue()


# ------------------------------------------------------------------ scenarios

def loan_info(loan, overrides=None):
    info = dict(loan["loanInfo"])
    info.update(overrides or {})
    return info


class Scenario:
    def __init__(self, name, loan_id, docs, extraction_required, loan_info, expect_code, **kw):
        self.name = name
        self.loan_id = loan_id
        self.correlation_id = f"eocr-exec-{uuid.uuid4().hex[:12]}"
        self.docs = docs                      # [(fileName, contentType, bytes)]
        self.extraction_required = extraction_required
        self.loan_info = loan_info
        self.expect_code = expect_code
        self.expect_validation = kw.get("expect_validation")
        self.expect_failed = kw.get("expect_failed", {})   # fileName -> reason substring
        self.flaky = kw.get("flaky", 0)
        self.hitl_reject = kw.get("hitl_reject", [])       # fields the HITL reviewer marks as mismatched
        self.control_override = kw.get("control_override")
        self.folder_batch_path = kw.get("folder_batch_path", False)  # send the folder, not the file (spec 4.1 wording)
        self.after = kw.get("after")               # another scenario that must finish first
        self.expect_description = kw.get("expect_description")
        self.done = threading.Event()
        self.result = {"scenario": name, "loanId": loan_id, "correlationId": self.correlation_id, "checks": [], "errors": [], "stages": [],
                       "documents": [{"fileName": n, "bytes": len(d)} for n, _, d in docs]}

    @property
    def folder(self):
        return f"loanId={self.loan_id}/correlationId={self.correlation_id}/"

    @property
    def control_key(self):
        return self.folder + "controlfile.json"

    @property
    def batch_path(self):
        return self.folder if self.folder_batch_path else self.control_key

    def ok(self, what):
        self.result["checks"].append(what)

    def err(self, what):
        self.result["errors"].append(what)

    def stage(self):
        control = {
            "loanInfo": {"loanId": self.loan_id, "correlationId": self.correlation_id, **self.loan_info},
            "extractionRequired": "true" if self.extraction_required else "false",
            "documents": [{"fileName": n, "contentType": ct} for n, ct, _ in self.docs],
        }
        if self.control_override:
            self.control_override(control)
        for name, ct, data in self.docs:
            s3.put_object(Bucket=INTAKE_BUCKET, Key=self.folder + name, Body=data, ContentType=ct)
        s3.put_object(Bucket=INTAKE_BUCKET, Key=self.control_key, Body=json.dumps(control, indent=2).encode(), ContentType="application/json")
        self.result["controlFile"] = control
        self.ok(f"staged s3://{INTAKE_BUCKET}/{self.folder} ({len(self.docs)} documents + controlfile.json); batchPath sent: {self.batch_path}")

    def onboard(self):
        body = {"loanId": self.loan_id, "correlationId": self.correlation_id, "batchPath": self.batch_path}
        code, resp = call("POST", "/integration/loan/onboarding", body, expect=202)
        if set(resp) != {"aceJobId", "status"} or resp["status"] != {"code": 202, "value": "ACCEPTED", "description": "Request accepted for processing."}:
            self.err(f"onboarding acknowledgement does not match spec 4.2: {resp}")
        self.job = resp["aceJobId"]
        if self.result.get("sameFilesAs") == self.job:
            self.err("a new eOCR execution (new correlationId) must get a new aceJobId")
        elif self.result.get("sameFilesAs"):
            self.ok(f"same package, new correlationId -> new aceJobId {self.job} (earlier job {self.result['sameFilesAs']})")
        self.result["aceJobId"] = self.job
        self.ok(f"onboarding -> HTTP 202 aceJobId={self.job}")
        if self.flaky:
            s3.put_object(Bucket=INTAKE_BUCKET, Key=f"{SIM_PREFIX}flaky/{self.job}", Body=json.dumps({"refuse": self.flaky}).encode())
            self.ok(f"eOCR callback endpoint will refuse the first {self.flaky} deliveries with 503")
        # idempotency: the same request again returns the same job
        _, again = call("POST", "/integration/loan/onboarding", body, expect=202)
        if again.get("aceJobId") != self.job:
            self.err(f"repeated onboarding returned a different job {again.get('aceJobId')}")
        else:
            self.ok("repeated onboarding request -> same aceJobId (idempotent)")

    def follow(self, timeout_s):
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            _, body = call("GET", f"/integration/loan/status/{self.job}", expect=200)
            errs = check_status_response(body, self.job)
            for e in errs:
                if e not in self.result["errors"]:
                    self.err(e)
            key = (body["workflow"].get("stage"), body["workflow"].get("state"), body["status"]["code"])
            if key != last:
                self.result["stages"].append({"at": datetime.now(timezone.utc).strftime("%H:%M:%S"), "stage": key[0], "state": key[1], "code": key[2], "value": body["status"]["value"]})
                log("status", scenario=self.name, aceJobId=self.job, stage=key[0], state=key[1], code=key[2])
                last = key
            if key[1] == "HITL_PENDING" and self.hitl_reject:
                self.hitl_review()
            if body["status"]["code"] in TERMINAL:
                self.final_status = body
                return body
            time.sleep(15)
        raise AssertionError(f"job {self.job} not terminal after {timeout_s}s; last={last}")

    def hitl_review(self):
        """Acts as the ACE HITL reviewer (not eOCR): confirms the mismatches ACE found."""
        _, review = call("POST", "/validate/reviewValidation", {"clientLoanNumber": self.loan_id, "adr": self.job}, expect=200)
        fields = [{"fieldName": f["fieldName"], "metadataValue": f.get("metadataValue"), "extractedValue": f.get("extractedValue"),
                   "isMatched": f["fieldName"] not in self.hitl_reject} for f in review["fieldDetails"]]
        self.ok(f"HITL review shown to reviewer: totalMatches={review['totalMatches']}, fields={[(f['fieldName'], f.get('isMatched')) for f in review['fieldDetails']]}")
        code, resp = call("POST", "/validate/updateValidation", {"clientLoanNumber": self.loan_id, "adr": self.job, "updatedFields": fields})
        self.ok(f"HITL reviewer confirmed mismatches {self.hitl_reject} -> HTTP {code}")
        self.hitl_reject = []

    def callbacks(self, timeout_s=900):
        deadline = time.time() + timeout_s
        prefix = f"{SIM_PREFIX}callbacks/{self.job}/"
        while time.time() < deadline:
            objs = sorted(o["Key"] for o in s3.list_objects_v2(Bucket=INTAKE_BUCKET, Prefix=prefix).get("Contents", []))
            records = [json.loads(s3.get_object(Bucket=INTAKE_BUCKET, Key=k)["Body"].read()) for k in objs]
            if any(r["answeredWith"] == 200 for r in records):
                return records
            time.sleep(10)
        raise AssertionError(f"no accepted callback for {self.job} within {timeout_s}s")

    def verify(self):
        final = self.final_status
        code = final["status"]["code"]
        if code != self.expect_code:
            self.err(f"terminal status {code} {final['status']['value']} ('{final['status']['description']}') != expected {self.expect_code} {STATUS[self.expect_code]}")
        else:
            self.ok(f"Status API terminal: {code} {final['status']['value']} - {final['status']['description']}")
        if self.expect_description and self.expect_description.lower() not in final["status"]["description"].lower():
            self.err(f"status.description '{final['status']['description']}' does not mention '{self.expect_description}'")
        records = self.callbacks()
        delivered = [r for r in records if r["answeredWith"] == 200]
        self.result["callbackAttempts"] = [{"attempt": r["attempt"], "at": r["receivedAt"], "answered": r["answeredWith"], "caller": r["callerPrincipal"]} for r in records]
        if len(delivered) != 1:
            self.err(f"expected exactly one accepted callback, got {len(delivered)}")
        cb = delivered[0]
        for e in cb["contractErrors"]:
            self.err(e)
        if not cb["contractErrors"]:
            self.ok("callback payload matches spec 5.1/6.x (fields, code/value, batchPath/failedDocuments rules, timestamp)")
        if not (cb["callerPrincipal"] or "").endswith("integration-task-role"):
            self.err(f"callback was not signed by the integration task role: {cb['callerPrincipal']}")
        else:
            self.ok(f"callback authenticated by IAM (Lattice) as {cb['callerPrincipal'].split('/')[-1]}")
        if self.flaky:
            refused = [r for r in records if r["answeredWith"] == 503]
            if len(refused) != self.flaky:
                self.err(f"expected {self.flaky} refused deliveries before success, saw {len(refused)}")
            elif len({r["idempotencyKey"] for r in records}) != 1:
                self.err("retries did not reuse the same Idempotency-Key")
            else:
                self.ok(f"intermittent outage: {self.flaky} deliveries refused (503), retried with backoff, delivered on attempt {cb['attempt']} with the same Idempotency-Key")
        p = cb["payload"]
        for field in ("status", "batchPath", "failedDocuments"):
            if p[field] != final[field]:
                self.err(f"callback {field} {p[field]} differs from Status API {final[field]}")
        self.result["callback"] = p
        if code == 1000:
            got = {f["documentName"]: f["reason"] for f in p["failedDocuments"]}
            for name, reason in self.expect_failed.items():
                if reason not in got.get(name, ""):
                    self.err(f"failedDocuments: expected '{name}' with reason '{reason}', got {got}")
            if not self.result["errors"]:
                self.ok(f"failedDocuments as expected: {got}")
        if p["batchPath"]:
            doc = json.loads(s3.get_object(Bucket=OUTPUT_BUCKET, Key=p["batchPath"])["Body"].read())
            names = {n for n, _, _ in self.docs}
            errs = check_response_file(doc, self.job, self.extraction_required, names, self.expect_validation)
            for e in errs:
                self.err(e)
            docs = doc["Documents"]
            summary = {
                "documentTypes": len(docs),
                "documents": sum(len(v) for v in docs.values()),
                "extractedFields": sum(len(i["extraction"]) for v in docs.values() for i in v),
                "files": sorted({i["fileName"] for v in docs.values() for i in v}),
                "validationStatus": doc["validationStatus"],
                "sample": next(iter(docs.items())),
            }
            self.result["responseFile"] = {"key": f"s3://{OUTPUT_BUCKET}/{p['batchPath']}", **summary}
            if not errs:
                self.ok(f"Response.json at s3://{OUTPUT_BUCKET}/{p['batchPath']} matches spec 6.4/6.5: "
                        f"{summary['documentTypes']} document types, {summary['documents']} documents, {summary['extractedFields']} extracted fields, validationStatus={doc['validationStatus']}")

    def run(self, timeout_s):
        if self.after is not None:
            self.after.done.wait(timeout_s)
            self.result["startedAfter"] = self.after.name
        if self.after is not None and self.after.result.get("aceJobId"):
            self.result["sameFilesAs"] = self.after.result["aceJobId"]
        started = time.time()
        try:
            self.stage()
            self.onboard()
            self.follow(timeout_s)
            self.verify()
        except Exception as exc:  # report, never hide
            self.err(f"{type(exc).__name__}: {exc}")
            log("scenario error", scenario=self.name, trace=traceback.format_exc()[-1500:])
        self.result["minutes"] = round((time.time() - started) / 60, 1)
        self.result["passed"] = not self.result["errors"]
        self.done.set()
        return self.result


def contract_checks():
    """Spec 4 / 7 edge cases that need no package processing."""
    res = {"scenario": "api-contract", "checks": [], "errors": []}
    try:
        code, body = call("GET", "/integration/loan/status/ADR-2026-999999999999")
        want = {"code": 4040, "value": "JOB_NOT_FOUND"}
        if code == 200 and {k: body["status"][k] for k in want} == want and body["workflow"] == {} and body["batchPath"] == "" and body["failedDocuments"] == []:
            res["checks"].append("unknown aceJobId -> HTTP 200, 4040 JOB_NOT_FOUND, workflow {}, empty batchPath/failedDocuments (spec 7.4)")
        else:
            res["errors"].append(f"unknown aceJobId -> HTTP {code} {body}")
        code, body = call("POST", "/integration/loan/onboarding", {"loanId": "X1", "correlationId": "c1", "batchPath": "not-a-control-file"})
        (res["checks"] if code == 400 else res["errors"]).append(f"malformed batchPath -> HTTP {code}")
        code, body = call("POST", "/integration/loan/onboarding", {"loanId": "X1", "correlationId": "c1", "batchPath": "loanId=X2/correlationId=c1/controlfile.json"})
        (res["checks"] if code == 400 else res["errors"]).append(f"batchPath loanId differs from request -> HTTP {code}")
        code, body = call("POST", "/integration/loan/onboarding", {"loanId": "X1", "correlationId": "c1"})
        (res["checks"] if code == 400 else res["errors"]).append(f"missing batchPath -> HTTP {code}")
    except Exception as exc:
        res["errors"].append(f"{type(exc).__name__}: {exc}")
    res["passed"] = not res["errors"]
    return res


def build_scenarios(loan, only):
    """Packages are page slices of the test loan that always contain its NOTE. Slice sizes are random
    per run and never equal across scenarios, because ACE marks a package with the same page count and
    OCR text as an earlier one as a DUPLICATE (tested on purpose by duplicate-resubmission)."""
    pkg = testdata(loan["file"])
    note_first, note_last = loan["notePages"]
    rnd = random.Random()

    def around_note(min_pages, max_pages):
        count = rnd.randint(min_pages, max_pages)
        first = rnd.randint(max(1, note_last - count + 1), note_first)
        return first, count

    lid = loan["loanId"]
    h_first, h_count = around_note(120, 159)
    happy_parts = [(f"{lid}_part{i + 1}.pdf", "application/pdf", data) for i, (data, _) in enumerate(split_pdf(page_slice(pkg, h_first, h_count), 3))]
    n_first, n_count = around_note(40, 69)
    no_extraction_doc = [(f"{lid}_package.pdf", "application/pdf", page_slice(pkg, n_first, n_count))]
    v_first, v_count = around_note(80, 109)
    validation_doc = [(f"{lid}_package.pdf", "application/pdf", page_slice(pkg, v_first, v_count))]
    cover = page_slice(pkg, 1, 2)
    wrong = {"loanAmount": "1.00", "sellerLoanNumber": "0000000000"}

    def mismatch_ids(control):
        control["loanInfo"]["loanId"] = "SOMEONE-ELSE"

    no_extraction = Scenario("no-extraction", lid, no_extraction_doc, False, loan_info(loan), 0, expect_validation="PASSED", folder_batch_path=True)
    scenarios = [
        Scenario("happy-path", lid, happy_parts, True, loan_info(loan), 0, expect_validation="PASSED", flaky=2),
        no_extraction,
        # same package, new eOCR execution: a new job. (ACE's own duplicate detection, loan.duplicate.logic.flag,
        # is off in this environment; when on, integration ends such a job as PROCESSING_FAILED naming the original.)
        Scenario("resubmission", lid, no_extraction_doc, False, loan_info(loan), 0, after=no_extraction, expect_validation="PASSED"),
        Scenario("validation-failed", lid, validation_doc, True, loan_info(loan, wrong), 2000,
                 expect_validation="FAILED", hitl_reject=["sellerLoanNumber", "loanAmount"]),
        Scenario("precheck-failed", lid, [("cover.pdf", "application/pdf", cover), ("locked.pdf", "application/pdf", password_protected_pdf()),
                                          ("broken.pdf", "application/pdf", b"%PDF-1.4\n1 0 obj garbage\n")], True, loan_info(loan), 1000,
                 expect_failed={"locked.pdf": "Password Protected", "broken.pdf": "Corrupted"}),
        Scenario("control-file-mismatch", lid, [("cover.pdf", "application/pdf", cover)], False, loan_info(loan), 1000,
                 expect_failed={"controlfile.json": "Loan ID Mismatch"}, control_override=mismatch_ids),
    ]
    slices = {"happy-path": (h_first, h_count), "no-extraction": (n_first, n_count), "resubmission": (n_first, n_count), "validation-failed": (v_first, v_count)}
    for s in scenarios:
        if s.name in slices:
            s.result["sourcePages"] = f"{loan['file']} pages {slices[s.name][0]}-{slices[s.name][0] + slices[s.name][1] - 1}"
    return [s for s in scenarios if not only or s.name in only or (s.name == "no-extraction" and "resubmission" in only)]

def run(args):
    if not INTEGRATION_URL:
        sys.exit("ACE_URL_INTEGRATION is not set")
    loan = json.loads(testdata("loan.json"))
    only = [x for x in (args.only or "").split(",") if x]
    results = [contract_checks()] if not only or "api-contract" in only else []
    scenarios = build_scenarios(loan, only)
    log("running scenarios", scenarios=[s.name for s in scenarios], integration=INTEGRATION_URL)
    with ThreadPoolExecutor(max_workers=max(1, len(scenarios))) as pool:
        results += list(pool.map(lambda s: s.run(args.timeout), scenarios))
    report = {"finishedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"), "passed": all(r["passed"] for r in results), "results": results}
    key = f"{SIM_PREFIX}reports/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    s3.put_object(Bucket=INTAKE_BUCKET, Key=key, Body=json.dumps(report, indent=2, default=str).encode(), ContentType="application/json")
    for r in results:
        log("RESULT", scenario=r["scenario"], passed=r["passed"], aceJobId=r.get("aceJobId"), minutes=r.get("minutes"), errors=r["errors"])
    print("REPORT " + json.dumps(report, default=str), flush=True)
    log("report written", key=f"s3://{INTAKE_BUCKET}/{key}", passed=report["passed"])
    sys.exit(0 if report["passed"] else 1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    r = sub.add_parser("run")
    r.add_argument("--only", help="comma-separated scenario names (default: all)")
    r.add_argument("--timeout", type=int, default=5400, help="seconds per scenario before it counts as failed")
    a = ap.parse_args()
    serve() if a.cmd == "serve" else run(a)
