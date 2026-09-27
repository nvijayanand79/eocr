"""Fake ACE integration service - LOCAL DEVELOPMENT ONLY.

Plays ACE's side of the ACE to eOCR Integration Specification v1.1 closely enough to click through the simulator
console on a laptop (with moto as S3): onboarding (202 + aceJobId, idempotent), Status API with workflow stages,
pre-check of the control file and PDFs, a HITL pause when the control file's loan amount / seller loan number look
wrong, *Response.json in the output bucket and the terminal callback with retries and one Idempotency-Key.
It is a stand-in for trying the console, not a model of ACE's real processing.

Faults, to check that the simulator reports them: put "simulate": "<fault>" in the control file's loanInfo
(the console's raw control-file editor), or use a loanId starting with REJECT for a refused onboarding.
  processing-failure  3000 PROCESSING_FAILED            no-callback     finishes, never calls back
  bad-callback        callback breaks the contract      missing-response  callback names a file that was never written
  status-mismatch     callback disagrees with Status    bad-response    Response.json breaks spec 6.4
  note-mismatch-passed  NOTE loan amount differs from the control file, validation still PASSED
  drop-document       the last submitted document is left out of Response.json
  text-content-type   Status API and callback say Content-Type text/plain
  fractional-time     timestamps with milliseconds (valid ISO 8601: must NOT be flagged)
  status-regress      the Status API goes from COMPLETED back to IN_PROGRESS before the callback
  bad-confidence      a confidence percentage for a page outside pageRange, another above 100
  extra-response      a second *Response.json next to the first      odd-name  result file not named *Response.json
A loanId starting with ACK200 is accepted with HTTP 200 instead of 202.
"""
import io
import json
import os
import random
import re
import threading
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from pypdf import PdfReader

REGION = os.environ.get("AWS_REGION", "us-east-1")
INTAKE = os.environ["ACE_BUCKET_INTAKE"]
OUTPUT = os.environ["ACE_BUCKET_OUTPUT"]
CALLBACK = os.environ.get("EOCR_CALLBACK_URL", "http://127.0.0.1:8080/eocr/callback")
STEP = float(os.environ.get("FAKE_ACE_STEP_SECONDS", "3"))
PORT = int(os.environ.get("FAKE_ACE_PORT", "9000"))
CALLER = "arn:aws:sts::000000000000:assumed-role/local-integration-task-role/fake-ace"
SUSPICIOUS = {"loanAmount": "1.00", "sellerLoanNumber": "0000000000"}  # values the fake's NOTE "disagrees" with

s3 = boto3.client("s3", region_name=REGION)
jobs, by_request, lock = {}, {}, threading.Lock()


def ts(job=None):
    now = datetime.now(timezone.utc)
    if job and job.get("fault") == "fractional-time":
        return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    return now.strftime("%Y-%m-%dT%H:%M:%SZ")


def set_status(job, stage, state, code, value, description, batch_path="", failed=None):
    job["status"] = {"aceJobId": job["id"], "workflow": {"stage": stage, "state": state},
                     "status": {"code": code, "value": value, "description": description},
                     "batchPath": batch_path, "failedDocuments": failed or [], "timestamp": ts(job)}


def precheck(job):
    folder = job["folder"]
    key = job["batchPath"]
    if key.endswith("/"):
        keys = [o["Key"] for o in s3.list_objects_v2(Bucket=INTAKE, Prefix=folder).get("Contents", []) if o["Key"].endswith(".json")]
        key = keys[0] if keys else folder + "controlfile.json"
    name = key.rsplit("/", 1)[-1]
    try:
        control = json.loads(s3.get_object(Bucket=INTAKE, Key=key)["Body"].read())
    except Exception:
        return None, [{"documentName": name, "reason": "Control File Missing Or Invalid"}]
    info = control.get("loanInfo") or {}
    if info.get("loanId") != job["loanId"] or info.get("correlationId") != job["correlationId"]:
        return control, [{"documentName": name, "reason": "Loan ID Mismatch" if info.get("loanId") != job["loanId"] else "Correlation ID Mismatch"}]
    failed = []
    job["pages"] = {}
    for d in control.get("documents") or []:
        fname = d.get("fileName")
        try:
            data = s3.get_object(Bucket=INTAKE, Key=folder + fname)["Body"].read()
        except Exception:
            failed.append({"documentName": fname, "reason": "File Not Found"})
            continue
        try:
            reader = PdfReader(io.BytesIO(data))
            if reader.is_encrypted:
                failed.append({"documentName": fname, "reason": "Password Protected"})
                continue
            job["pages"][fname] = len(reader.pages)
        except Exception:
            failed.append({"documentName": fname, "reason": "Corrupted"})
    if not control.get("documents"):
        failed.append({"documentName": name, "reason": "No Documents Listed"})
    return control, failed


def response_file(job, control, extraction):
    docs, total = {}, 0
    info = control["loanInfo"]
    types = [("NOTE", "101", "Promissory Note"), ("DEED_OF_TRUST", "205", "Deed of Trust"), ("CLOSING_DISCLOSURE", "310", "Closing Disclosure")]
    fields = {"NOTE": {"loanAmount": info.get("loanAmount"), "borrowerLastName": info.get("borrowerLastName")},
              "DEED_OF_TRUST": {"sellerLoanNumber": info.get("sellerLoanNumber"), "recordingDate": "2026-01-15"},
              "CLOSING_DISCLOSURE": {"closingDate": "2026-01-10", "interestRate": "6.125"}}
    for fname, pages in job["pages"].items():
        size = s3.head_object(Bucket=INTAKE, Key=job["folder"] + fname)["ContentLength"]
        total += size
        bounds = [1 + round(i * pages / 3) for i in range(4)]
        for i, (key, tid, tname) in enumerate(types):
            first, last = bounds[i], max(bounds[i], bounds[i + 1] - 1)
            docs.setdefault(key, []).append({
                "fileName": fname, "docTypeId": tid, "docTypeName": tname, "pageRange": f"{first}-{last}" if last > first else str(first),
                "confidencePercentage": {str(p): str(random.randint(88, 99)) for p in range(first, last + 1)}, "duplicatePagesOf": "",
                "extraction": {k: {"Value": v, "cr": str(random.randint(85, 99))} for k, v in fields[key].items()} if extraction else {}})
    return {"aceJobId": job["id"], "extractionRequired": control.get("extractionRequired", "false"),
            "validationStatus": job.get("validation", "PASSED"), "Documents": docs,
            "fileName": "_".join(job["pages"]), "fileSize": str(total)}


def callback(job, override=None):
    payload = {k: job["status"][k] for k in ("aceJobId", "status", "batchPath", "failedDocuments", "timestamp")}
    body = json.dumps(override(payload) if override else payload).encode()
    key = str(uuid.uuid4())
    for attempt in range(8):
        req = urllib.request.Request(CALLBACK, data=body, method="POST", headers={
            "Content-Type": "text/plain" if job.get("fault") == "text-content-type" else "application/json", "Idempotency-Key": key, "x-amzn-lattice-identity": f"Principal={CALLER}; SessionName=fake"})
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                if r.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(min(2 ** attempt, 30))
    return False


def process(job):
    try:
        time.sleep(STEP)
        set_status(job, "PRECHECK", "RUNNING", 4000, "IN_PROGRESS", "Pre-check is currently in progress.")
        time.sleep(STEP)
        control, failed = precheck(job)
        if failed:
            set_status(job, "PRECHECK", "FAILED", 1000, "PRECHECK_FAILED", "One or more documents failed PreCheck validation.", failed=failed)
            return callback(job)
        set_status(job, "CLASSIFICATION", "RUNNING", 4000, "IN_PROGRESS", "Classification is currently in progress.")
        time.sleep(STEP)
        set_status(job, "EXTRACTION", "RUNNING", 4000, "IN_PROGRESS", "Extraction is currently in progress.")
        time.sleep(STEP)
        info = control["loanInfo"]
        fault = job["fault"] = info.get("simulate", "")
        if fault == "processing-failure":
            set_status(job, "EXTRACTION", "FAILED", 3000, "PROCESSING_FAILED", "Extraction service returned an error (simulated).")
            return callback(job)
        suspicious = [k for k, v in SUSPICIOUS.items() if info.get(k) == v]
        if suspicious:
            job["review"] = [{"fieldName": k, "metadataValue": info.get(k), "extractedValue": {"loanAmount": "250000.00", "sellerLoanNumber": "7700112233"}[k],
                              "isMatched": False} for k in SUSPICIOUS] + [{"fieldName": "borrowerLastName", "metadataValue": info.get("borrowerLastName"),
                                                                        "extractedValue": info.get("borrowerLastName"), "isMatched": True}]
            set_status(job, "EXTRACTION", "HITL_PENDING", 4000, "IN_PROGRESS", "Waiting for manual review.")
            job["decided"].wait()
        name = f"{job['id']}/{job['loanId']}_Response.json" if fault != "odd-name" else f"{job['id']}/{job['loanId']}_result.json"
        if job.get("mismatched"):
            job["validation"] = "FAILED"
            desc = ", ".join({"loanAmount": "Loan Amount Mismatch", "sellerLoanNumber": "Seller Loan Number Mismatch"}.get(f, f"{f} Mismatch") for f in job["mismatched"]) + "."
            s3.put_object(Bucket=OUTPUT, Key=name, Body=json.dumps(response_file(job, control, False), indent=2).encode(), ContentType="application/json")
            set_status(job, "VALIDATION", "FAILED", 2000, "VALIDATION_FAILED", desc, batch_path=name)
            return callback(job)
        extraction = control.get("extractionRequired") == "true"
        doc = response_file(job, control, extraction)
        if fault == "note-mismatch-passed":
            for item in doc["Documents"].get("NOTE", []):
                if "loanAmount" in item["extraction"]:
                    item["extraction"]["loanAmount"]["Value"] = "199999.00"
        if fault == "drop-document" and len(job["pages"]) > 1:
            gone = list(job["pages"])[-1]
            doc["Documents"] = {k: [i for i in v if i["fileName"] != gone] for k, v in doc["Documents"].items()}
            doc["Documents"] = {k: v for k, v in doc["Documents"].items() if v}
        if fault == "bad-confidence":
            item = next(iter(doc["Documents"].values()))[0]
            item["confidencePercentage"]["999"] = "95"
        if fault == "extra-response":
            s3.put_object(Bucket=OUTPUT, Key=f"{job['id']}/{job['loanId']}_old_Response.json", Body=b"{}", ContentType="application/json")
        if fault == "bad-response":
            doc["aceJobId"] = "ADR-SOMEONE-ELSE"
            next(iter(doc["Documents"].values()))[0]["pageRange"] = "pages one to three"
        if fault != "missing-response":
            s3.put_object(Bucket=OUTPUT, Key=name, Body=json.dumps(doc, indent=2).encode(), ContentType="application/json")
        set_status(job, "COMPLETED", "CLIENT_CALLBACK_PENDING", 0, "COMPLETED", "Processing completed successfully.", batch_path=name)
        if fault == "no-callback":
            return
        if fault == "status-regress":  # terminal, then back to in progress, then terminal again before calling back
            time.sleep(STEP * 4)
            set_status(job, "EXTRACTION", "RUNNING", 4000, "IN_PROGRESS", "Extraction is currently in progress.")
            time.sleep(STEP * 4)
            set_status(job, "COMPLETED", "CLIENT_CALLBACK_PENDING", 0, "COMPLETED", "Processing completed successfully.", batch_path=name)
        override = None
        if fault == "bad-callback":
            override = lambda p: {**p, "status": {"code": "0", "value": "SUCCESS", "description": ""}, "timestamp": "27/09/2026 10:00", "output": {"batchPath": p["batchPath"]}}  # noqa: E731
        elif fault == "status-mismatch":
            set_status(job, "COMPLETED", "CLIENT_CALLBACK_PENDING", 3000, "PROCESSING_FAILED", "Status store disagrees (simulated).")
            override = lambda p: {**p, "status": {"code": 0, "value": "COMPLETED", "description": "Processing completed successfully."}, "batchPath": name}  # noqa: E731
        if callback(job, override) and fault != "status-mismatch":
            set_status(job, "COMPLETED", "CLIENT_CALLBACK_DONE", 0, "COMPLETED", "Processing completed successfully.", batch_path=name)

    except Exception as exc:
        set_status(job, "PROCESSING", "FAILED", 3000, "PROCESSING_FAILED", f"Unexpected processing failure: {exc}")
        callback(job)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def reply(self, code, body, content_type="application/json"):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def body(self):
        try:
            return json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        except ValueError:
            return {}

    def do_GET(self):
        m = re.fullmatch(r"/integration/loan/status/([^/]+)", self.path)
        if not m:
            return self.reply(404, {"error": "not found"})
        job = jobs.get(m.group(1))
        if not job:
            return self.reply(200, {"aceJobId": m.group(1), "workflow": {}, "status": {"code": 4040, "value": "JOB_NOT_FOUND",
                                    "description": "No processing request found for the specified aceJobId."}, "batchPath": "", "failedDocuments": [], "timestamp": ts()})
        self.reply(200, job["status"], "text/plain" if job.get("fault") == "text-content-type" else "application/json")

    def do_POST(self):
        b = self.body()
        if self.path == "/integration/loan/onboarding":
            loan, cid, path = b.get("loanId"), b.get("correlationId"), b.get("batchPath")
            if not (loan and cid and path):
                return self.reply(400, {"error": "loanId, correlationId and batchPath are required"})
            if str(loan).startswith("REJECT"):
                return self.reply(400, {"error": f"loanId {loan} is not onboarded in ACE (simulated rejection)"})
            folder = f"loanId={loan}/correlationId={cid}/"
            if not (path == folder or re.fullmatch(re.escape(folder) + r"[^/]+\.json", path)):
                return self.reply(400, {"error": f"batchPath must be {folder} or {folder}<control>.json"})
            with lock:
                job = jobs.get(by_request.get((loan, cid)))
                if not job:
                    job = {"id": f"ADR-{datetime.now(timezone.utc):%Y}-{random.randint(10**11, 10**12 - 1)}", "loanId": loan, "correlationId": cid,
                           "batchPath": path, "folder": folder, "decided": threading.Event()}
                    set_status(job, "COLLATION", "QUEUED", 202, "ACCEPTED", "Request accepted and queued for processing.")
                    jobs[job["id"]] = job
                    by_request[(loan, cid)] = job["id"]
                    threading.Thread(target=process, args=(job,), daemon=True).start()
            return self.reply(200 if str(loan).startswith("ACK200") else 202, {"aceJobId": job["id"], "status": {"code": 202, "value": "ACCEPTED", "description": "Request accepted for processing."}})
        job = jobs.get(b.get("adr"))
        if self.path == "/validate/reviewValidation" and job and job.get("review"):
            return self.reply(200, {"clientLoanNumber": job["loanId"], "adr": job["id"], "fieldDetails": job["review"],
                                    "totalMatches": sum(f["isMatched"] for f in job["review"])})
        if self.path == "/validate/updateValidation" and job and job.get("review"):
            job["mismatched"] = [f["fieldName"] for f in b.get("updatedFields") or [] if not f.get("isMatched")]
            job["decided"].set()
            return self.reply(200, {"updated": True})
        self.reply(404, {"error": "not found"})


if __name__ == "__main__":
    print(f"fake ACE on :{PORT}, callbacks to {CALLBACK}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
