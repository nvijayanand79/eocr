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

import store

REGION = os.environ.get("AWS_REGION", "us-east-1")
INTEGRATION_URL = os.environ.get("ACE_URL_INTEGRATION", "").rstrip("/")
INTAKE_BUCKET = os.environ.get("ACE_BUCKET_INTAKE", "")
OUTPUT_BUCKET = os.environ.get("ACE_BUCKET_OUTPUT", "")
TESTDATA_BUCKET = os.environ.get("ACE_BUCKET_CONFIG", "")
TESTDATA_PREFIX = os.environ.get("SIM_TESTDATA_PREFIX", "test-data/eocr/")
SIM_PREFIX = "eocr-sim/"  # simulator bookkeeping inside the intake bucket (eOCR's own area)
PORT = int(os.environ.get("PORT", "8080"))
CONSOLE_PORT = int(os.environ.get("SIM_CONSOLE_PORT", "8081"))   # 0 switches the console off
CONSOLE_HOST = os.environ.get("SIM_CONSOLE_HOST", "127.0.0.1")   # loopback: reached through an SSM port-forward

STATUS = {0: "COMPLETED", 1000: "PRECHECK_FAILED", 2000: "VALIDATION_FAILED", 3000: "PROCESSING_FAILED",
          4000: "IN_PROGRESS", 202: "ACCEPTED", 4040: "JOB_NOT_FOUND"}
TERMINAL = {0, 1000, 2000, 3000}
TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|\+00:00)$")  # ISO 8601, UTC (spec 5.1)


def is_utc_timestamp(value):
    if not TIMESTAMP.match(str(value or "")):
        return False
    try:
        datetime.fromisoformat(re.sub(r"(\.\d{1,6})\d*", r"\1", str(value)).replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def json_content_type(value, where):
    """Spec 4, 5 and 7: every body is application/json."""
    if not str(value or "").lower().split(";")[0].strip() == "application/json":
        return [f"{where}: Content-Type is {value!r}, not application/json"]
    return []

s3 = boto3.client("s3", region_name=REGION)
_session = boto3.Session(region_name=REGION)


def log(msg, **kv):
    print(json.dumps({"ts": datetime.now(timezone.utc).strftime("%H:%M:%S"), "msg": msg, **kv}, default=str), flush=True)


# ------------------------------------------------------------------ contract checks (spec sections 5-7)

def check_status_object(status, where):
    errs = []
    if not isinstance(status, dict) or set(status) != {"code", "value", "description"}:
        return [f"{where}: status must be exactly {{code, value, description}}: {status}"]
    if not isinstance(status["code"], int) or isinstance(status["code"], bool):
        errs.append(f"{where}: status.code must be a number, got {type(status['code']).__name__}")
    elif STATUS.get(status["code"]) != status["value"]:
        errs.append(f"{where}: status.code {status['code']} does not match status.value {status['value']}")
    if not str(status.get("description") or "").strip():
        errs.append(f"{where}: status.description is empty")
    return errs


def check_output_rules(code, job_id, batch_path, failed, where):
    errs = []
    if code not in STATUS:
        return errs  # the unknown code is reported by check_status_object; its output rules are undefined
    if code in (0, 2000):
        if not re.fullmatch(re.escape(job_id) + r"/[^/]+\.json", batch_path or ""):
            errs.append(f"{where}: {STATUS[code]} needs batchPath '<aceJobId>/<file>.json', got '{batch_path}'")
        elif not str(batch_path).endswith("Response.json"):
            errs.append(f"{where}: result file '{batch_path}' is not named *Response.json (spec 6.4)")
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
    if not is_utc_timestamp(payload.get("timestamp")):
        errs.append(f"callback: timestamp '{payload.get('timestamp')}' is not an ISO 8601 UTC time (e.g. 2026-09-11T07:53:00Z)")
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
    if not is_utc_timestamp(body.get("timestamp")):
        errs.append(f"status API: timestamp '{body.get('timestamp')}' is not an ISO 8601 UTC time")
    return errs


def _pages(page_range):
    pages = set()
    for part in str(page_range or "").split(","):
        m = re.fullmatch(r"\s*(\d+)\s*(?:-\s*(\d+))?\s*", part)
        if not m:
            return set()
        pages.update(range(int(m.group(1)), int(m.group(2) or m.group(1)) + 1))
    return pages


def check_response_file(doc, job_id, extraction_required, file_names, expect_validation, min_types=3):
    """Spec 6.4 / 6.5. expect_validation: a value, a tuple of allowed values, or None (not checked).
    min_types: document types that must carry extracted fields when extraction ran (whole-package extraction)."""
    errs = []
    order = ["aceJobId", "extractionRequired", "validationStatus", "Documents", "fileName", "fileSize"]
    if list(doc) != order:
        errs.append(f"Response.json top-level keys {list(doc)} != {order}")
    if doc.get("aceJobId") != job_id:
        errs.append("Response.json aceJobId mismatch")
    if isinstance(extraction_required, str):  # the control file's own value, as sent
        sent = extraction_required
        extraction_required = {"true": True, "false": False}.get(sent)  # None: the control file sent neither, no extraction rule applies
    else:
        sent = "true" if extraction_required else "false"
    if doc.get("extractionRequired") not in ("true", "false"):
        errs.append(f"Response.json extractionRequired {doc.get('extractionRequired')!r} is not \"true\" or \"false\"")
    elif sent in ("true", "false") and doc.get("extractionRequired") != sent:
        errs.append(f"Response.json extractionRequired {doc.get('extractionRequired')!r} but the control file sent {sent!r}")
    if doc.get("validationStatus") not in ("PASSED", "FAILED", "NA"):
        errs.append(f"Response.json validationStatus {doc.get('validationStatus')!r} is not PASSED, FAILED or NA")
    allowed = expect_validation if isinstance(expect_validation, tuple) else (expect_validation,)
    if expect_validation is not None and doc.get("validationStatus") not in allowed:
        errs.append(f"Response.json validationStatus {doc.get('validationStatus')} != {expect_validation}")
    docs = doc.get("Documents")
    if not isinstance(docs, dict) or not docs:
        return errs + ["Response.json Documents must be a non-empty object"]
    fields = ["fileName", "docTypeId", "docTypeName", "pageRange", "confidencePercentage", "duplicatePagesOf", "extraction"]
    extracted = 0
    extracted_types = set()
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
            else:
                pages = _pages(item["pageRange"])
                for page, pct in item["confidencePercentage"].items():
                    if not str(page).isdigit() or (pages and int(page) not in pages):
                        errs.append(f"Documents['{doc_type}'] confidencePercentage page {page!r} is outside pageRange '{item['pageRange']}'")
                        break
                    try:
                        ok = 0 <= float(str(pct).rstrip("%")) <= 100
                    except ValueError:
                        ok = False
                    if not ok:
                        errs.append(f"Documents['{doc_type}'] confidencePercentage {pct!r} for page {page} is not a percentage")
                        break
            if not isinstance(item["extraction"], dict):
                errs.append(f"Documents['{doc_type}'] extraction must be an object")
                continue
            for name, field in item["extraction"].items():
                extracted += 1
                extracted_types.add(doc_type)
                if not isinstance(field, dict) or list(field) != ["Value", "cr"]:
                    errs.append(f"Documents['{doc_type}'].extraction['{name}'] must be {{Value, cr}}: {field}")
    if (extraction_required is False or expect_validation == "FAILED") and extracted:
        errs.append(f"Response.json has {extracted} extracted fields although extraction must be empty")
    # full extraction, not just the NOTE extracted for validation: several document types carry fields
    if extraction_required is True and expect_validation != "FAILED" and len(extracted_types) < min_types:
        errs.append(f"Response.json has extracted fields in only {sorted(extracted_types)} although whole-package extraction was required")
    if not str(doc.get("fileSize", "")).isdigit():
        errs.append(f"Response.json fileSize {doc.get('fileSize')!r} is not a byte count")
    return errs


def response_summary(doc):
    docs = doc.get("Documents") or {}
    items = [(k, i) for k, v in docs.items() if isinstance(v, list) for i in v if isinstance(i, dict)]
    return {
        "documentTypes": len(docs),
        "documents": len(items),
        "extractedFields": sum(len(i.get("extraction") or {}) for _, i in items),
        "documentTypesWithFields": sorted({k for k, i in items if i.get("extraction")}),
        "files": sorted({str(i.get("fileName")) for _, i in items}),
        "validationStatus": doc.get("validationStatus"),
        "rows": [{"documentType": k, "fileName": i.get("fileName"), "docTypeId": i.get("docTypeId"), "docTypeName": i.get("docTypeName"),
                  "pageRange": i.get("pageRange"), "duplicatePagesOf": i.get("duplicatePagesOf"),
                  "fields": {n: (f or {}).get("Value") if isinstance(f, dict) else f for n, f in (i.get("extraction") or {}).items()}}
                 for k, i in items],
    }


# ------------------------------------------------------------------ NOTE and document validation, as eOCR sees it

FIELD_LABELS = {"loanAmount": "Loan Amount", "sellerLoanNumber": "Seller Loan Number", "borrowerLastName": "Borrower Last Name"}


def _norm(value):
    text = str(value if value is not None else "").strip()
    try:
        return round(float(re.sub(r"[$,\s]", "", text)), 2)
    except ValueError:
        return re.sub(r"\s+", " ", text).upper()


def _validate(b):
    """Freeze the validation view on the execution when it closes (after result and outcome are known)."""
    b["validation"] = validation_view(b)
    for f in b["validation"]["findings"]:
        add_event(b, "validation-finding", f)


def validation_view(batch):
    """Control-file loan data vs what ACE found, ACE's verdict, the HITL reviewer's decision, and what happened to each
    submitted document. Findings are problems eOCR would raise with ACE: they are counted like spec deviations."""
    outcome = batch.get("outcome") or {}
    st = (batch.get("status") or {}).get("status") or {}
    code = outcome.get("code", st.get("code"))
    description = str(outcome.get("description") or st.get("description") or "")
    summary = (batch.get("result") or {}).get("summary") or {}
    rows = summary.get("rows") or []
    status = summary.get("validationStatus") or ({2000: "FAILED"}.get(code) if code is not None else None)
    control = ((batch.get("controlFile") or {}).get("loanInfo")) or batch.get("loanInfo") or {}
    hitl = {f.get("fieldName"): f for f in ((batch.get("hitl") or {}).get("fields") or [])}
    decided = {f.get("fieldName"): f.get("isMatched") for f in (((batch.get("hitl") or {}).get("decision") or {}).get("fields") or [])}

    def found(field):
        for want_note in (True, False):
            for r in rows:
                if ("NOTE" in str(r.get("documentType", "")).upper()) != want_note:
                    continue
                for name, value in (r.get("fields") or {}).items():
                    if name.lower() == field.lower():
                        return value, r.get("documentType")
        return None, None

    fields, findings = [], []
    for name, expected in control.items():
        if name in ("loanId", "correlationId", "simulate"):
            continue
        label = FIELD_LABELS.get(name, re.sub(r"(?<!^)([A-Z])", r" \1", name).title())
        value, source = found(name)
        h = hitl.get(name) or {}
        if value is None and h.get("extractedValue") is not None:
            value, source = h.get("extractedValue"), "HITL review"
        ours = None if value is None else ("match" if _norm(value) == _norm(expected) else "mismatch")
        named = f"{label} Mismatch".lower() in description.lower() or (code == 2000 and label.lower() in description.lower())
        ace = "mismatch" if named else ("match" if status == "PASSED" else None)
        if status == "PASSED" and ours == "mismatch":
            findings.append(f"ACE passed validation, but {label} on the {source or 'NOTE'} is {value!r} while the control file says {expected!r}")
        if code == 2000 and ours == "match" and named:
            findings.append(f"ACE reports a {label} mismatch, but the extracted value {value!r} equals the control file")
        fields.append({"field": name, "label": label, "control": expected, "extracted": value, "source": source, "ours": ours, "ace": ace,
                       "hitlExtracted": h.get("extractedValue"), "hitlDecision": decided.get(name)})
    if code == 2000 and not any(f["ace"] == "mismatch" for f in fields):
        findings.append(f"VALIDATION_FAILED without naming a mismatched field the control file carries: '{description}'")
    if status == "FAILED" and code not in (2000, None):
        findings.append(f"Response.json says validationStatus FAILED but the outcome is {code} {outcome.get('value')}")

    failed = {f.get("documentName"): f.get("reason") for f in (outcome.get("failedDocuments") or []) if isinstance(f, dict)}
    uploaded = {d["fileName"]: d for d in batch.get("documents") or []}
    listed = [d.get("fileName") for d in ((batch.get("controlFile") or {}).get("documents") or []) if isinstance(d, dict)]
    documents = []
    for name in list(uploaded) + [n for n in listed if n not in uploaded]:
        d = uploaded.get(name, {"fileName": name, "bytes": None})
        mine = [r for r in rows if r.get("fileName") == d["fileName"]]
        precheck = (f"failed: {failed[name]}" if name in failed else "passed" if code in (0, 2000)
                    else "no failure reported" if code == 1000 else None)
        documents.append({"fileName": d["fileName"], "bytes": d.get("bytes"), "precheck": precheck,
                          "note": None if name in uploaded and name in listed else "listed in the control file, not uploaded" if name not in uploaded
                          else "uploaded, not listed in the control file",
                          "types": sorted({r.get("documentType") for r in mine}), "pages": ", ".join(str(r.get("pageRange")) for r in mine),
                          "extractedFields": sum(len(r.get("fields") or {}) for r in mine),
                          "duplicates": sorted({str(r.get("duplicatePagesOf")) for r in mine if r.get("duplicatePagesOf")})})
        if code in (0, 2000) and rows and not mine and d["fileName"] not in failed:
            findings.append(f"{d['fileName']} passed pre-check but is not in Response.json")
    for name in failed:
        if name not in uploaded and name not in listed and name != batch.get("controlFileName"):
            findings.append(f"failedDocuments names {name}, which was not submitted")
    ran = code in (0, 2000) or status in ("PASSED", "FAILED")
    return {"validationStatus": status, "ran": ran, "stoppedAt": None if ran or code is None else outcome.get("value") or st.get("value"),
            "description": description, "fields": fields, "documents": documents, "findings": findings,
            "hitlUsed": bool(hitl), "complete": batch.get("state") == "CLOSED"}


# ------------------------------------------------------------------ spec checklist: every clause, passed / failed / not applicable

def spec_checklist(batch):
    """One row per clause of the integration spec that an execution exercises, with the deviations found for it."""
    cb = batch.get("callback") or {}
    result = batch.get("result") or {}
    outcome = batch.get("outcome") or {}
    status_errs = list(batch.get("contractErrors") or [])
    cb_errs = list(cb.get("contractErrors") or [])
    res_errs = list(result.get("errors") or [])
    closed, submitted = batch["state"] == "CLOSED", bool(batch.get("aceJobId"))
    finished = outcome.get("value") not in (None, "ABANDONED")

    def pick(errs, *prefixes, exclude=()):
        return [e for e in errs if e.lower().startswith(prefixes) and not e.lower().startswith(exclude)]

    def row(clause, title, applies, errors, done=True, pending_text="not yet"):
        state = "n/a" if not applies else "failed" if errors else "passed" if done else "pending"
        return {"clause": clause, "check": title, "result": state, "errors": errors, "note": pending_text if state == "pending" else None}

    staged = batch["state"] not in ("DRAFT",)
    rows = [
        row("3.1", "Package staged as loanId=<loanId>/correlationId=<execution>/ with the control file and documents", staged, [], staged),
        row("3.2", "Control file: loanInfo (loanId, correlationId, sellerLoanNumber, loanAmount, borrowerLastName), extractionRequired \"true\"/\"false\", documents with fileName and contentType",
            staged, list(batch.get("controlIssues") or []), staged),
        row("4, 4.2", "Onboarding accepted: HTTP 202, application/json, exactly {aceJobId, status 202 ACCEPTED}", batch["state"] not in ("DRAFT", "STAGED"),
            pick(status_errs, "onboarding") + ([f"ACE refused the request: {(e.get('message') or '')}; response {((e.get('data') or {}).get('response'))}"
                                                for e in (batch.get("events") or []) if e.get("type") == "rejected"][-1:] if batch["state"] == "REJECTED" else []),
            submitted or batch["state"] == "REJECTED"),
        row("7.1-7.4, mapping", "Status API: exact fields, workflow stage/state, numeric code matching value, batchPath / failedDocuments per code, ISO 8601 UTC timestamp, application/json",
            submitted, pick(status_errs, "status api"), bool(batch.get("stages"))),
        row("7", "Status progresses without going back or changing a terminal outcome, and knows the job", submitted, pick(status_errs, "status progression"), closed),
        row("5", "Terminal callback delivered to the eOCR endpoint", submitted and finished or (submitted and not closed),
            [] if cb or not closed else [f"no callback received; closed by {batch.get('closedBy')}"], bool(cb), "waiting for ACE"),
        row("5.1, 5.2, 6.1-6.3", "Callback payload: flat fields, numeric terminal code matching value, batchPath / failedDocuments rules, ISO 8601 UTC timestamp, application/json",
            bool(cb) or (submitted and not closed), pick(cb_errs, "callback"), bool(cb), "waiting for ACE"),
        row("5, 7", "Callback agrees with the Status API (status, batchPath, failedDocuments)", bool(cb), pick(status_errs, "callback differs"), bool(cb)),
        row("3.3 step 7, 6.4, 6.5", "*Response.json retrieved: one per batch, exact schema, Documents items, pageRange and confidence by page, extraction rules, validationStatus",
            bool(outcome.get("batchPath")) or (submitted and not closed), res_errs, bool(result.get("key")), "written on COMPLETED / VALIDATION_FAILED"),
        row("6.3, 6.5, 3.2", "NOTE validation and document results consistent with the control file and the package",
            closed and finished, list((batch.get("validation") or {}).get("findings") or []), closed),
    ]
    covered = {e for r in rows for e in r["errors"]}
    other = [e for e in status_errs + cb_errs + res_errs if e not in covered]
    if other:
        rows.append(row("other", "Other deviations", True, other))
    return rows


# ------------------------------------------------------------------ expected outcome of a test, and its verdict

EOCR_SIDE = ("3.1", "3.2")  # checklist rows about the package eOCR staged (not ACE's behaviour)


def evaluate_expectation(batch):
    """What the tester said should happen against what did. None when no expectation was set."""
    exp = batch.get("expectation")
    if not exp:
        return None
    state, o = batch["state"], batch.get("outcome") or {}
    done = state in ("CLOSED", "REJECTED")
    checks = []

    def add(what, expected, actual, ok):
        checks.append({"what": what, "expected": expected, "actual": actual if done else None, "ok": bool(done and ok)})

    code = exp.get("code")
    if code is not None:
        actual = "refused by ACE" if state == "REJECTED" else f"{o.get('code')} {o.get('value')}" if o else None
        add("Outcome", "refused by ACE" if code == "REJECTED" else f"{code} {STATUS.get(code, '')}".strip(), actual,
            (state == "REJECTED") if code == "REJECTED" else (state == "CLOSED" and o.get("code") == code))
    got = {d.get("documentName"): str(d.get("reason") or "") for d in o.get("failedDocuments") or [] if isinstance(d, dict)}
    for f in exp.get("failedDocuments") or []:
        name, reason = f.get("documentName"), str(f.get("reason") or "")
        add(f"Failed document {name}", reason or "listed as failed", got.get(name, "not listed"),
            name in got and reason.lower() in got[name].lower())
    if exp.get("validationStatus"):
        vs = (batch.get("validation") or {}).get("validationStatus")
        add("NOTE validation", exp["validationStatus"], vs or "not reported", vs == exp["validationStatus"])
    for term in exp.get("descriptionContains") or []:
        add("ACE's description mentions", term, o.get("description") or "(empty)", term.lower() in str(o.get("description") or "").lower())
    if exp.get("minCallbackAttempts"):
        att = (batch.get("callback") or {}).get("attempt")
        add("Callback delivered after eOCR refused it", f"attempt {exp['minCallbackAttempts']} or later", f"attempt {att}" if att else "no callback",
            bool(att) and att >= int(exp["minCallbackAttempts"]))
    if exp.get("specClean", True):
        devs = sum(len(r["errors"]) for r in spec_checklist(batch) if r["result"] == "failed" and r["clause"] not in EOCR_SIDE)
        devs += len((batch.get("validation") or {}).get("findings") or [])
        add("ACE followed the spec", "no deviations or validation findings", f"{devs} found" if devs else "none", devs == 0)
    status = "pending" if not done else "passed" if checks and all(c["ok"] for c in checks) else "failed"
    return {"status": status, "title": exp.get("title"), "preset": exp.get("preset"), "checks": checks}


def _judge(b):
    """Freeze the verdict when the job ends."""
    v = evaluate_expectation(b)
    if v:
        b["verdict"] = v
        add_event(b, "verdict", f"test {v['status'].upper()}: " + ("outcome as expected" if v["status"] == "passed"
                  else "; ".join(f"{c['what']}: expected {c['expected']}, got {c['actual']}" for c in v["checks"] if not c["ok"])))


# ------------------------------------------------------------------ where the job is, and what went wrong (for people)

ACE_STAGE_ORDER = ["COLLATION", "PRECHECK", "CLASSIFICATION", "EXTRACTION", "VALIDATION", "COMPLETED"]
STAGE_LABELS = {"COLLATION": "Collation", "PRECHECK": "Pre-check", "CLASSIFICATION": "Classification", "EXTRACTION": "Extraction",
                "VALIDATION": "Validation", "COMPLETED": "Completed", "PROCESSING": "Processing"}
FIX = {
    "password protected": "Remove the password from the PDF (or export an unprotected copy), then submit again as a new job.",
    "corrupted": "The file cannot be opened as a PDF. Re-export or re-scan it, then submit again as a new job.",
    "file not found": "The control file lists a document that is not in the S3 folder. Upload it, or leave it out of the control file.",
    "loan id mismatch": "loanInfo.loanId in the control file must equal the loan of the folder and of the request.",
    "correlation id mismatch": "loanInfo.correlationId in the control file must equal the execution's correlationId.",
    "control file missing or invalid": "Write the control file again (valid JSON, named as in batchPath).",
    "no documents listed": "Add at least one document to the package.",
}


def _iso_ts(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def stage_track(batch):
    """The job as a line of steps: eOCR prepares and submits, ACE works through its stages, eOCR receives and closes."""
    steps = []
    state, outcome = batch["state"], batch.get("outcome") or {}
    code = outcome.get("code")
    cb, result = batch.get("callback") or {}, batch.get("result") or {}
    closed, submitted = state == "CLOSED", bool(batch.get("aceJobId"))

    def add(key, label, side, status, at=None, note=None):
        steps.append({"key": key, "label": label, "side": side, "status": status, "at": at, "note": note})

    staged_at = next((e["at"] for e in reversed(batch.get("events") or []) if e.get("type") == "staged"), None)
    docs = batch.get("documents") or []
    add("documents", "Documents", "eOCR", "done" if docs and state != "DRAFT" or (docs and staged_at) else "current" if state == "DRAFT" else "done",
        min((d.get("uploadedAt") for d in docs if d.get("uploadedAt")), default=batch.get("createdAt")), f"{len(docs)} file(s)")
    add("control", "Control file", "eOCR", "done" if state not in ("DRAFT",) else "current" if docs else "upcoming", staged_at,
        batch.get("controlFileName"))
    add("submit", "Submitted to ACE", "eOCR", "failed" if state == "REJECTED" else "done" if submitted else "current" if state == "STAGED" else "upcoming",
        batch.get("submittedAt"), batch.get("aceJobId") or ("refused by ACE" if state == "REJECTED" else None))

    groups = []
    for s in batch.get("stages") or []:
        stage = (s.get("key") or [None])[0] or "QUEUED"
        if groups and groups[-1]["stage"] == stage:
            groups[-1]["states"].append(s["key"][1])
            groups[-1]["last"] = s
        else:
            groups.append({"stage": stage, "at": s.get("at"), "states": [s["key"][1]], "last": s})
    final = ((batch.get("status") or {}).get("status") or {}).get("code")
    terminal = final in TERMINAL or code in (0, 1000, 2000, 3000)
    failed_code = code if code in (1000, 2000, 3000) else final if final in (1000, 2000, 3000) else None
    for i, g in enumerate(groups):
        last = i == len(groups) - 1
        states = [x for x in g["states"] if x]
        status = "done"
        if last and failed_code:
            status = "failed"
        elif last and not terminal and not closed:
            status = "waiting" if "HITL_PENDING" in states else "current"
        add(f"ace-{g['stage']}", STAGE_LABELS.get(g["stage"], g["stage"].title()), "ACE", status, g["at"],
            " → ".join(dict.fromkeys(states)) or None)
    if submitted and not groups:
        add("ace-wait", "ACE processing", "ACE", "current" if not closed else "done", None, "waiting for the first status")
    if submitted and not terminal and not closed:
        seen = [ACE_STAGE_ORDER.index(g["stage"]) for g in groups if g["stage"] in ACE_STAGE_ORDER]
        for stage in ACE_STAGE_ORDER[(max(seen) + 1) if seen else 0:]:
            add(f"ace-{stage}", STAGE_LABELS[stage], "ACE", "upcoming", None, "expected")

    if cb:
        add("callback", "Callback received", "eOCR", "failed" if cb.get("contractErrors") else "done", cb.get("receivedAt"),
            f"attempt {cb.get('attempt')}" + (" · off-spec" if cb.get("contractErrors") else ""))
    elif closed and outcome.get("value") != "ABANDONED":
        add("callback", "Callback received", "eOCR", "failed", None, "never received")
    elif submitted and not closed:
        add("callback", "Callback received", "eOCR", "current" if terminal else "upcoming", None, "waiting for ACE" if terminal else None)
    elif state != "REJECTED":
        add("callback", "Callback received", "eOCR", "upcoming")
    if outcome.get("batchPath") or (submitted and not closed and state != "REJECTED"):
        add("result", "Result file checked", "eOCR", "failed" if result.get("key") and not result.get("ok") else "done" if result.get("key")
            else "upcoming", batch.get("closedAt") if result.get("key") else None, (result.get("key") or "").rsplit("/", 1)[-1] or None)
    if state != "REJECTED":
        add("closed", "Closed", "eOCR", "done" if closed else "upcoming", batch.get("closedAt"),
            {"callback": "on ACE's callback", "reconciliation": "from the Status API", "manual": "by hand"}.get(batch.get("closedBy")))
    for i, st in enumerate(steps):  # how long each step took: until the next step that has a time
        nxt = next((x["at"] for x in steps[i + 1:] if x.get("at")), None)
        a, b = _iso_ts(st.get("at")), _iso_ts(nxt)
        st["seconds"] = round(b - a) if a is not None and b is not None and b >= a else None
    return steps


def explain(batch):
    """Plain-language account of where the job stands or what went wrong, which items are affected and what to do."""
    state, outcome = batch["state"], batch.get("outcome") or {}
    code, st = outcome.get("code"), (batch.get("status") or {}).get("status") or {}
    wf = (batch.get("status") or {}).get("workflow") or {}
    v = batch.get("validation") or validation_view(batch)
    checklist = spec_checklist(batch)
    deviations = [{"clause": r["clause"], "check": r["check"], "errors": r["errors"]} for r in checklist if r["result"] == "failed"
                  and not r["clause"].startswith("3.")]
    ours = [e for r in checklist if r["result"] == "failed" and r["clause"].startswith("3.") and r["clause"] != "3.3 step 7, 6.4, 6.5"
            for e in r["errors"]]
    last_stage = STAGE_LABELS.get(wf.get("stage"), wf.get("stage")) if wf.get("stage") else None
    job = batch.get("aceJobId")
    r = {"tone": "info", "headline": "", "summary": "", "stage": None, "items": [], "next": [], "deviations": deviations, "packageIssues": ours}

    if state == "DRAFT":
        r.update(tone="idle", headline="Not submitted yet", summary="Finish the package: add documents and write the control file.",
                 next=["Open the setup and continue where you left off."])
    elif state == "STAGED":
        r.update(tone="info", headline="Ready to submit", summary="The documents and the control file are in S3. Submit the job to ACE.",
                 next=["Review the S3 folder, then submit."])
    elif state == "REJECTED":
        ev = next((e for e in reversed(batch.get("events") or []) if e.get("type") == "rejected"), {})
        resp = (ev.get("data") or {}).get("response")
        reason = (resp or {}).get("error") or (resp or {}).get("message") if isinstance(resp, dict) else resp
        r.update(tone="bad", stage="Submission", headline="ACE refused the submission",
                 summary=f"The onboarding request was answered with {ev.get('message', 'an error')}.",
                 items=[{"subject": "Onboarding request", "problem": str(reason or "no reason given"),
                         "fix": "Check the loan exists in ACE, the batchPath and the ACE endpoint; then submit again."}],
                 next=["Correct the request and submit again (the package is kept).", "If the loan should be known to ACE, give the ACE team the loanId and the time."])
    elif state != "CLOSED":
        if wf.get("state") == "HITL_PENDING":
            r.update(tone="warn", stage=last_stage, headline="Waiting for a HITL review",
                     summary="ACE paused for a person to confirm loan data it could not match.",
                     next=["Open the review, confirm or reject each field, and send the decision."])
        elif batch.get("callbackOverdue"):
            r.update(tone="bad", stage="Callback", headline="ACE finished but has not called back",
                     summary=f"ACE reports {st.get('code')} {st.get('value')}, but no callback has reached the eOCR endpoint.",
                     next=["Wait a little longer, or close the job from the Status API (Close → reconciliation).",
                           f"Ask the ACE team why the callback for {job} was not delivered."])
        elif st.get("code") in TERMINAL:
            r.update(tone="info", stage="Callback", headline=f"ACE finished ({st.get('value')}); waiting for its callback",
                     summary="The job closes on its own when the callback arrives.")
        else:
            r.update(tone="info", stage=last_stage, headline=f"ACE is processing{': ' + last_stage if last_stage else ''}",
                     summary=st.get("description") or "Waiting for the first status from ACE.")
    elif outcome.get("value") == "ABANDONED":
        r.update(tone="warn", headline="Closed by hand before ACE finished", summary=outcome.get("description") or "",
                 next=["Resubmit as a new job if it still needs processing."])
    elif code not in (0, 1000, 2000, 3000):
        r.update(tone="bad", stage="Callback", headline="ACE sent an invalid callback",
                 summary=f"Its status was code {code!r} / value {outcome.get('value')!r}, which the spec does not allow. "
                         f"ACE's Status API says {st.get('code')} {st.get('value')}.",
                 next=["Report the deviations below to the ACE team with the aceJobId."])
    elif code == 1000:
        failed = [f for f in outcome.get("failedDocuments") or [] if isinstance(f, dict)]
        r.update(tone="bad", stage="Pre-check", headline=f"Pre-check failed: {len(failed)} item(s) rejected",
                 summary=outcome.get("description") or "",
                 items=[{"subject": f.get("documentName"), "problem": f.get("reason"),
                         "fix": FIX.get(str(f.get("reason", "")).lower(), "Correct the file and submit again as a new job.")} for f in failed],
                 next=["Fix the items listed above, then use “Resubmit as new job”: it copies the package, and you replace or remove the bad files before submitting.",
                       "No result file is written for a pre-check failure."])
    elif code == 2000:
        bad = [f for f in v.get("fields") or [] if f.get("ace") == "mismatch" or f.get("hitlDecision") is False]
        r.update(tone="bad", stage="Validation", headline="NOTE validation failed: " + (", ".join(f["label"] for f in bad) or "loan data does not match"),
                 summary=outcome.get("description") or "",
                 items=[{"subject": f["label"], "problem": f"control file says {f['control']!r}, the NOTE shows {f['extracted']!r}" if f.get("extracted") is not None
                         else f"control file says {f['control']!r}; ACE found a different value",
                         "fix": f"If {f['control']!r} is wrong, correct {f['field']} in the control file; if it is right, the NOTE needs review."} for f in bad],
                 next=["Correct the loan data and resubmit as a new job, or confirm the NOTE is the wrong document.",
                       "The result file lists the documents ACE classified; it has no extracted fields (spec 6.5)."])
    elif code == 3000:
        r.update(tone="bad", stage=last_stage or "Processing", headline="ACE could not process the package",
                 summary=outcome.get("description") or "Unexpected processing or system failure.",
                 items=[{"subject": f"ACE job {job}", "problem": outcome.get("description") or "processing failure",
                         "fix": "Usually not caused by the package. Retry as a new job; if it repeats, report it."}],
                 next=["Resubmit as a new job.", f"If it fails again, give the ACE team the aceJobId {job} and the time it failed."])
    else:
        problems = deviations or v.get("findings")
        r.update(tone="warn" if problems else "ok", stage=None,
                 headline="Completed, but the result has problems" if problems else "Completed",
                 summary=outcome.get("description") or "",
                 items=[{"subject": "Validation", "problem": f, "fix": "Report to the ACE team with the aceJobId."} for f in v.get("findings") or []],
                 next=["Report the problems below to the ACE team with the aceJobId."] if problems else ["Nothing to do. The result file is in the Files tab."])
    if state == "CLOSED" and batch.get("closedBy") == "reconciliation":
        r["summary"] = (r["summary"] + " Closed from the Status API: ACE never called back.").strip()
    return r


# ------------------------------------------------------------------ batch ledger (eOCR's record of each execution)
#
# One JSON document per eOCR execution (correlationId) in s3://<intake>/eocr-sim/batches/, updated with S3
# conditional writes so the console, the callback receiver and scenario runs (a separate task) never lose each
# other's updates. Lifecycle, as eOCR sees it:
#   DRAFT -> STAGED (control file written) -> SUBMITTED (202 + aceJobId) -> IN_PROGRESS (Status API)
#         -> CALLBACK_RECEIVED -> CLOSED (result retrieved and checked)         REJECTED: onboarding refused

BATCH_PREFIX = f"{SIM_PREFIX}batches/"
JOB_PREFIX = f"{SIM_PREFIX}jobs/"          # aceJobId -> correlationId
ACTIVE_PREFIX = f"{SIM_PREFIX}active/"     # executions the console tracker polls
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
SAFE_FILE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._() -]{0,199}")
TRACKED = {"SUBMITTED", "IN_PROGRESS", "CALLBACK_RECEIVED"}
CALLBACK_GRACE_S = int(os.environ.get("SIM_CALLBACK_GRACE_SECONDS", "600"))
MAX_EVENTS = 400


class Conflict(Exception):
    """The requested action does not fit the execution's current state."""


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_correlation_id():
    return f"eocr-exec-{uuid.uuid4().hex[:12]}"


def _precondition_failed(exc):
    return isinstance(exc, store.PreconditionFailed) or getattr(exc, "response", {}).get("Error", {}).get("Code") in ("PreconditionFailed", "ConditionalRequestConflict")


OPERATOR = threading.local()  # the console sets .name for the request it is serving: who did it goes into every event


def operator():
    return getattr(OPERATOR, "name", None)


def add_event(batch, kind, message, **data):
    by = {"by": operator()} if operator() else {}
    batch.setdefault("events", []).append({"at": now_iso(), "type": kind, "message": message, **by, **({"data": data} if data else {})})
    del batch["events"][:-MAX_EVENTS]


def load_batch(cid):
    try:
        return store.get_json(f"{BATCH_PREFIX}{cid}.json")
    except KeyError:
        return None, None


def _save(batch, etag):
    store.put_json(f"{BATCH_PREFIX}{batch['correlationId']}.json", batch, **({"if_match": etag} if etag else {"if_none_match": True}))
    marker = f"{ACTIVE_PREFIX}{batch['correlationId']}"
    if batch["state"] in TRACKED and batch.get("track", True):
        store.get().put(marker, b"", "text/plain")
    else:
        store.get().delete(marker)


def create_batch(loan_id, correlation_id=None, loan_info=None, extraction_required=True, control_file_name="controlfile.json", source="console", track=True):
    cid = correlation_id or new_correlation_id()
    for label, value in (("loanId", loan_id), ("correlationId", cid)):
        if not SAFE_ID.fullmatch(str(value or "")):
            raise ValueError(f"{label} must be 1-128 characters of letters, digits, '.', '_' or '-'")
    if not (SAFE_FILE.fullmatch(control_file_name) and control_file_name.endswith(".json")):
        raise ValueError("control file name must be a plain file name ending in .json")
    batch = {
        "correlationId": cid, "loanId": loan_id, "source": source, "track": track, "state": "DRAFT",
        "createdAt": now_iso(), "updatedAt": now_iso(),
        "folder": f"loanId={loan_id}/correlationId={cid}/", "controlFileName": control_file_name,
        "loanInfo": {k: str(v) for k, v in (loan_info or {}).items() if k not in ("loanId", "correlationId")},
        "extractionRequired": bool(extraction_required), "controlOverride": None,
        "documents": [], "controlFile": None, "batchPath": None, "aceJobId": None, "flaky": 0,
        "status": None, "stages": [], "contractErrors": [], "callback": None, "result": None, "outcome": None,
        "closedAt": None, "closedBy": None, "createdBy": operator(), "submittedBy": None, "closedByUser": None, "events": [],
    }
    add_event(batch, "created", f"execution {cid} created for loan {loan_id} ({source})")
    try:
        _save(batch, None)
    except Exception as exc:
        if _precondition_failed(exc):
            raise Conflict(f"correlationId {cid} already exists") from exc
        raise
    return batch


def update_batch(cid, change):
    """Read-modify-write with an S3 ETag precondition; change(batch) mutates it and may raise Conflict."""
    for attempt in range(10):
        batch, etag = load_batch(cid)
        if batch is None:
            raise KeyError(cid)
        before = json.dumps(batch, sort_keys=True, default=str)
        change(batch)
        if json.dumps(batch, sort_keys=True, default=str) == before:
            return batch  # nothing changed: no write, so readers see a stable updatedAt
        batch["updatedAt"] = now_iso()
        try:
            _save(batch, etag)
            return batch
        except Exception as exc:
            if not _precondition_failed(exc):
                raise
            time.sleep(0.1 * (attempt + 1) + random.random() * 0.2)
    raise RuntimeError(f"could not update execution {cid}: too many concurrent writers")


_summaries = {}  # batch key -> (ETag, summary): the ledger only grows, so re-read only what changed


def _summary(b):
    st = b.get("status") or {}
    return {k: b.get(k) for k in ("correlationId", "loanId", "aceJobId", "state", "source", "createdAt", "updatedAt", "submittedAt",
                                  "closedAt", "closedBy", "callbackOverdue", "resubmissionOf", "outcomeRecord", "aceUrl",
                                  "createdBy", "submittedBy", "closedByUser")} \
        | {"status": st.get("status"), "workflow": st.get("workflow"), "outcome": b.get("outcome"),
           "documents": len(b.get("documents") or []), "resultOk": (b.get("result") or {}).get("ok"),
           "callbackAt": (b.get("callback") or {}).get("receivedAt"), "callbackAttempts": (b.get("callback") or {}).get("attempt"),
           "contractErrors": len(set(b.get("contractErrors") or []) | set((b.get("callback") or {}).get("contractErrors") or [])
                                 | set((b.get("result") or {}).get("errors") or [])),
           "validationFindings": len((b.get("validation") or {}).get("findings") or []),
           "verdict": (b.get("verdict") or {}).get("status") or ("pending" if b.get("expectation") and b["state"] not in ("CLOSED", "REJECTED") else None),
           "testTitle": (b.get("expectation") or {}).get("title"),
           "validationStatus": (b.get("validation") or {}).get("validationStatus"),
           "lastEvent": ((b.get("events") or [None])[-1] or {}).get("message")}


def list_batches(q="", since_days=None, limit=2000):
    """Every execution in the ledger, newest first; q matches loanId, correlationId or aceJobId."""
    objs = [o for o in store.get().list(BATCH_PREFIX) if o.key.endswith(".json")]
    if since_days:
        cutoff = datetime.now(timezone.utc).timestamp() - float(since_days) * 86400
        objs = [o for o in objs if o.modified.timestamp() >= cutoff]
    objs.sort(key=lambda o: o.modified, reverse=True)
    q = (q or "").strip().lower()
    out = []
    for o in objs:
        cached = _summaries.get(o.key)
        if not cached or cached[0] != o.etag:
            try:
                cached = (o.etag, _summary(store.get_json(o.key)[0]))
            except KeyError:
                continue
            _summaries[o.key] = cached
        summary = cached[1]
        if q and not any(q in str(summary.get(k) or "").lower() for k in ("loanId", "correlationId", "aceJobId", "createdBy", "submittedBy")):
            continue
        out.append(summary)
        if len(out) >= limit:
            break
    return out


_callbacks, _job_cid = {}, {}  # callback records never change once written; job -> execution never changes once known


STALE_HOURS = 24


def needs_attention(summary):
    """Open but stuck (no callback after ACE finished, HITL review waiting, no change for STALE_HOURS), rejected, or off-spec."""
    open_ = summary["state"] in ("SUBMITTED", "IN_PROGRESS", "CALLBACK_RECEIVED")
    try:
        stale = open_ and time.time() - datetime.fromisoformat(summary["updatedAt"].replace("Z", "+00:00")).timestamp() > STALE_HOURS * 3600
    except (AttributeError, ValueError):
        stale = False
    return bool((open_ and (summary.get("callbackOverdue") or (summary.get("workflow") or {}).get("state") == "HITL_PENDING")) or stale
                or summary["state"] == "REJECTED" or summary.get("contractErrors") or summary.get("validationFindings") or summary.get("verdict") == "failed")


def list_callbacks(limit=500):
    """Every callback delivery the eOCR endpoint received (all jobs, known or not), newest first."""
    objs = [o for o in store.get().list(f"{SIM_PREFIX}callbacks/") if o.key.endswith(".json")]
    objs.sort(key=lambda o: o.modified, reverse=True)
    out = []
    for o in objs[:limit]:
        if o.key not in _callbacks:
            _callbacks[o.key] = store.get_json(o.key)[0]
        rec = _callbacks[o.key]
        job = o.key.split("/")[-2]
        if job not in _job_cid:
            cid = batch_for_job(job)
            if cid:
                _job_cid[job] = cid
        out.append({"aceJobId": job, "correlationId": _job_cid.get(job), **rec})
    return out


def index_job(job_id, cid):
    store.put_json(f"{JOB_PREFIX}{job_id}.json", {"correlationId": cid})


def batch_for_job(job_id):
    try:
        return store.get_json(f"{JOB_PREFIX}{job_id}.json")[0]["correlationId"]
    except KeyError:
        return None


def callback_records(job_id):
    keys = sorted(o.key for o in store.get().list(f"{SIM_PREFIX}callbacks/{job_id}/"))
    return [store.get_json(k)[0] for k in keys]


CONTROL_LOAN_FIELDS = ("loanId", "correlationId", "sellerLoanNumber", "loanAmount", "borrowerLastName")


def control_file_issues(control, batch, present=None):
    """Spec 3.1/3.2 as eOCR must produce it. present: {fileName: contentType} of the objects in the folder."""
    issues = []
    if not isinstance(control, dict):
        return ["control file is not a JSON object"]
    missing = [k for k in ("loanInfo", "extractionRequired", "documents") if k not in control]
    if missing:
        issues.append(f"control file lacks {missing}")
    info = control.get("loanInfo") if isinstance(control.get("loanInfo"), dict) else {}
    empty = [k for k in CONTROL_LOAN_FIELDS if not str(info.get(k) or "").strip()]
    if empty:
        issues.append(f"loanInfo is missing or empty: {empty}")
    if info.get("loanId") not in (None, "") and info.get("loanId") != batch["loanId"]:
        issues.append(f"loanInfo.loanId {info.get('loanId')!r} differs from the folder's loanId {batch['loanId']!r}")
    if info.get("correlationId") not in (None, "") and info.get("correlationId") != batch["correlationId"]:
        issues.append(f"loanInfo.correlationId {info.get('correlationId')!r} differs from the folder's correlationId {batch['correlationId']!r}")
    if control.get("extractionRequired") not in ("true", "false"):
        issues.append(f"extractionRequired is {control.get('extractionRequired')!r}; spec 3.2 wants the string \"true\" or \"false\"")
    docs = control.get("documents")
    if not isinstance(docs, list) or not docs:
        issues.append("documents lists no documents")
    else:
        for d in docs:
            if not isinstance(d, dict) or not d.get("fileName") or not d.get("contentType"):
                issues.append(f"documents entry needs fileName and contentType: {d}")
            elif present is not None and d["fileName"] not in present:
                issues.append(f"{d['fileName']} is listed but not in the folder")
            elif present is not None and present[d["fileName"]] and present[d["fileName"]].split(";")[0] != d["contentType"]:
                issues.append(f"{d['fileName']}: contentType {d['contentType']!r} differs from the uploaded object's {present[d['fileName']]!r}")
        if present is not None:
            extra = sorted(set(present) - {d.get("fileName") for d in docs if isinstance(d, dict)} - {batch.get("controlFileName")})
            if extra:
                issues.append(f"in the folder but not listed in the control file: {extra}")
    return issues


def build_control_file(batch):
    if batch.get("controlOverride") is not None:
        return batch["controlOverride"]
    return {
        "loanInfo": {"loanId": batch["loanId"], "correlationId": batch["correlationId"], **batch["loanInfo"]},
        "extractionRequired": "true" if batch["extractionRequired"] else "false",
        "documents": [{"fileName": d["fileName"], "contentType": d["contentType"]} for d in batch["documents"]],
    }


def retrieve_result(batch, code, batch_path):
    """Spec 3.3 step 7: fetch *Response.json when batchPath is populated and check it (spec 6.4/6.5)."""
    if not batch_path:
        return {"ok": True, "key": None, "errors": [], "summary": None}
    try:
        doc = json.loads(s3.get_object(Bucket=OUTPUT_BUCKET, Key=batch_path)["Body"].read())
    except Exception as exc:
        return {"ok": False, "key": f"s3://{OUTPUT_BUCKET}/{batch_path}", "errors": [f"cannot read Response.json: {type(exc).__name__}: {exc}"], "summary": None}
    expect = "FAILED" if code == 2000 else ("PASSED", "NA") if code == 0 else None
    sent = (batch.get("controlFile") or {}).get("extractionRequired") if isinstance(batch.get("controlFile"), dict) else None
    errs = check_response_file(doc, batch["aceJobId"], sent if isinstance(sent, str) else batch["extractionRequired"], {d["fileName"] for d in batch["documents"]}, expect, min_types=0) \
        if isinstance(doc, dict) else ["Response.json is not a JSON object"]
    try:
        files = [o["Key"] for o in s3.list_objects_v2(Bucket=OUTPUT_BUCKET, Prefix=f"{batch['aceJobId']}/").get("Contents", []) if o["Key"].endswith("Response.json")]
        if len(files) > 1:
            errs.append(f"Response.json: {len(files)} result files for one batch, spec 6.4 says one: {files}")
    except Exception as exc:
        log("output listing failed", aceJobId=batch.get("aceJobId"), error=str(exc))
    return {"ok": not errs, "key": f"s3://{OUTPUT_BUCKET}/{batch_path}", "errors": errs, "summary": response_summary(doc) if isinstance(doc, dict) else None}


SETTINGS_KEY = f"{SIM_PREFIX}settings.json"
ACE_URL_ALLOWED = [u.strip().rstrip("/") for u in os.environ.get("SIM_ACE_URL_ALLOWED", "").split(",") if u.strip()]


def settings():
    try:
        return store.get_json(SETTINGS_KEY)[0]
    except KeyError:
        return {}


def check_ace_url(url):
    """An ACE integration endpoint: http(s)://host[:port][/base], and inside SIM_ACE_URL_ALLOWED when that is set."""
    url = str(url or "").strip().rstrip("/")
    if not re.fullmatch(r"https?://[A-Za-z0-9.-]+(:\d{1,5})?(/[A-Za-z0-9._~/-]*)?", url):
        raise ValueError(f"ACE endpoint must look like https://host[:port][/path], got '{url}'")
    if ACE_URL_ALLOWED and not any(url == a or url.startswith(a + "/") or url.startswith(a + ":") for a in ACE_URL_ALLOWED):
        raise ValueError(f"ACE endpoint {url} is not in SIM_ACE_URL_ALLOWED")
    return url


def default_ace_url():
    """The console's default ACE endpoint: the one saved from the console, else ACE_URL_INTEGRATION."""
    return settings().get("aceUrl") or INTEGRATION_URL


def save_default_ace_url(url):
    doc = settings()
    if url:
        doc.update(aceUrl=check_ace_url(url), aceUrlChangedAt=now_iso())
    else:
        doc.pop("aceUrl", None)
    store.put_json(SETTINGS_KEY, doc)
    return doc


STORE_LABEL = f"db:{store.SCHEMA}.object/" if store.MODE == "db" else f"s3://{INTAKE_BUCKET}/"
RECORD_PREFIX = f"{SIM_PREFIX}records/"   # immutable per-execution records: what was submitted, how it ended


def _put_record(cid, name, doc):
    key = f"{RECORD_PREFIX}{cid}/{name}"
    store.put_json(key, doc)
    return key


def store_submission(cid, request, http_status, response):
    """Freeze exactly what was submitted: request/response, the control file as ACE will read it, every document's size and ETag."""
    batch, _ = load_batch(cid)
    docs = []
    for d in batch["documents"]:
        key = batch["folder"] + d["fileName"]
        try:
            h = s3.head_object(Bucket=INTAKE_BUCKET, Key=key)
            docs.append({"fileName": d["fileName"], "contentType": d["contentType"], "s3Key": f"s3://{INTAKE_BUCKET}/{key}",
                         "bytes": h["ContentLength"], "etag": h["ETag"].strip('"'), "lastModified": h["LastModified"].isoformat()})
        except Exception as exc:
            docs.append({"fileName": d["fileName"], "s3Key": f"s3://{INTAKE_BUCKET}/{key}", "missing": f"{type(exc).__name__}: {exc}"})
    control_key = batch["folder"] + batch["controlFileName"]
    try:
        control = json.loads(s3.get_object(Bucket=INTAKE_BUCKET, Key=control_key)["Body"].read())
    except Exception as exc:
        control = {"unreadable": f"{type(exc).__name__}: {exc}"}
    at = now_iso()
    record = {"correlationId": cid, "loanId": batch["loanId"], "source": batch["source"], "submittedAt": at,
              "aceUrl": batch.get("aceUrl") or INTEGRATION_URL, "submittedBy": operator() or batch.get("submittedBy"),
              "endpoint": "POST /integration/loan/onboarding", "request": request, "httpStatus": http_status, "response": response,
              "aceJobId": response.get("aceJobId") if isinstance(response, dict) else None,
              "controlFileKey": f"s3://{INTAKE_BUCKET}/{control_key}", "controlFile": control, "documents": docs}
    key = _put_record(cid, f"submission-{at.replace(':', '')}.json", record)

    def change(b):
        b["submission"] = {"key": key, "at": at, "httpStatus": http_status, "aceJobId": record["aceJobId"], "documents": len(docs),
                           "bytes": sum(d.get("bytes", 0) for d in docs)}
        add_event(b, "stored", f"submission stored: {STORE_LABEL}{key}")
    update_batch(cid, change)
    return record


def store_outcome(cid):
    """Freeze how the execution ended, with a copy of *Response.json that outlives the output bucket's retention."""
    batch, _ = load_batch(cid)
    if not batch or batch["state"] != "CLOSED" or batch.get("outcomeRecord"):
        return
    copy = None
    source = (batch.get("outcome") or {}).get("batchPath")
    if source:
        try:
            copy = f"{RECORD_PREFIX}{cid}/{source.rsplit('/', 1)[-1]}"
            store.get().put(copy, s3.get_object(Bucket=OUTPUT_BUCKET, Key=source)["Body"].read(), "application/json")
        except Exception as exc:
            log("Response.json copy failed", correlationId=cid, error=f"{type(exc).__name__}: {exc}")
            copy = None
    record = {"correlationId": cid, "loanId": batch["loanId"], "aceJobId": batch.get("aceJobId"), "submission": batch.get("submission"),
              "closedAt": batch["closedAt"], "closedBy": batch["closedBy"], "closedByUser": batch.get("closedByUser"), "outcome": batch["outcome"],
              "callbackAccepted": batch.get("callback"), "callbackAttempts": callback_records(batch["aceJobId"]) if batch.get("aceJobId") else [],
              "statusApiAtClose": batch.get("status"), "stages": batch.get("stages"), "result": batch.get("result"),
              "validation": batch.get("validation"), "hitl": batch.get("hitl"), "expectation": batch.get("expectation"), "verdict": batch.get("verdict"),
              "specChecklist": spec_checklist(batch),
              "responseFileCopy": f"{STORE_LABEL}{copy}" if copy else None,
              "contractErrors": sorted(set(batch.get("contractErrors") or []) | set((batch.get("callback") or {}).get("contractErrors") or [])
                                       | set((batch.get("result") or {}).get("errors") or []))}
    key = _put_record(cid, "outcome.json", record)

    def change(b):
        b["outcomeRecord"] = key
        b["responseCopy"] = copy
        add_event(b, "stored", f"outcome stored: {STORE_LABEL}{key}" + (f"; Response.json copied to {STORE_LABEL}{copy}" if copy else ""))
    update_batch(cid, change)


def list_records(cid):
    objs = store.get().list(f"{RECORD_PREFIX}{cid}/")
    return [{"name": o.key.rsplit("/", 1)[-1], "key": f"{STORE_LABEL}{o.key}", "bytes": o.size,
             "storedAt": o.modified.isoformat()} for o in sorted(objs, key=lambda o: o.key)]


def read_record(cid, name):
    if not SAFE_FILE.fullmatch(name):
        raise KeyError(name)
    try:
        return store.get_json(f"{RECORD_PREFIX}{cid}/{name}")[0]
    except KeyError:
        raise KeyError(name) from None


def record_status(batch, body, source="tracker", content_type=None):
    """Fold one Status API response into the execution: stage transitions, contract errors, terminal notice."""
    errs = check_status_response(body, batch["aceJobId"])
    if content_type is not None:
        errs += json_content_type(content_type, "status API")
    prev = ((batch.get("status") or {}).get("status") or {}).get("code")
    new = (body.get("status") or {}).get("code")
    if new == 4040 and batch.get("aceJobId"):
        errs.append(f"status progression: the Status API reports 4040 JOB_NOT_FOUND for {batch['aceJobId']}, which ACE accepted")
    elif prev in TERMINAL and new not in TERMINAL:
        errs.append(f"status progression: went back from terminal {prev} {STATUS.get(prev)} to {new} {STATUS.get(new)}")
    elif prev in TERMINAL and new in TERMINAL and new != prev:
        errs.append(f"status progression: terminal outcome changed from {prev} {STATUS.get(prev)} to {new} {STATUS.get(new)}")
    for e in errs:
        if e not in batch["contractErrors"]:
            batch["contractErrors"].append(e)
            add_event(batch, "contract-error", e)
    wf, st = body.get("workflow") or {}, body.get("status") or {}
    key = [wf.get("stage"), wf.get("state"), st.get("code")]
    if not batch["stages"] or batch["stages"][-1]["key"] != key:
        batch["stages"].append({"at": now_iso(), "key": key, "value": st.get("value"), "description": st.get("description")})
        add_event(batch, "status", f"{wf.get('stage')}/{wf.get('state')} -> {st.get('code')} {st.get('value')}: {st.get('description')}", via=source)
    if {k: v for k, v in (batch.get("status") or {}).items() if k != "timestamp"} != {k: v for k, v in body.items() if k != "timestamp"}:
        batch["status"] = body
    if batch["state"] == "SUBMITTED" and st.get("code") in (4000, 202):
        batch["state"] = "IN_PROGRESS"
    if st.get("code") in TERMINAL and batch["state"] in ("SUBMITTED", "IN_PROGRESS") and not batch.get("terminalSeenAt"):
        batch["terminalSeenAt"] = time.time()
        add_event(batch, "terminal", f"Status API reports {st.get('code')} {st.get('value')}; waiting for ACE's callback")
    if batch.get("terminalSeenAt") and batch["state"] != "CLOSED" and not batch.get("callback") and not batch.get("callbackOverdue") \
            and time.time() - batch["terminalSeenAt"] > CALLBACK_GRACE_S:
        batch["callbackOverdue"] = True
        add_event(batch, "warning", f"no callback {CALLBACK_GRACE_S}s after the Status API went terminal; close by reconciliation if it never comes")


def refresh_status(cid, source="tracker"):
    batch, _ = load_batch(cid)
    if batch is None:
        raise KeyError(cid)
    if not batch.get("aceJobId"):
        raise Conflict("execution has no aceJobId yet")
    meta = {}
    code, body = call("GET", f"/integration/loan/status/{batch['aceJobId']}", base=batch.get("aceUrl"), meta=meta, job=cid)
    if code != 200 or not isinstance(body, dict):
        return update_batch(cid, lambda b: add_event(b, "error", f"Status API -> HTTP {code}", body=body))
    return update_batch(cid, lambda b: record_status(b, body, source, meta.get("contentType")))


def on_callback(job_id, record):
    """Called by the callback receiver for every delivery attempt: trace it and close the execution on the accepted one."""
    cid = batch_for_job(job_id)
    if not cid:
        return
    p = record["payload"]
    st = p.get("status") if isinstance(p.get("status"), dict) else {}
    info = {"attempt": record["attempt"], "answeredWith": record["answeredWith"], "idempotencyKey": record["idempotencyKey"], "caller": record["callerPrincipal"]}
    if record["answeredWith"] != 200:
        update_batch(cid, lambda b: add_event(b, "callback-refused", f"callback attempt {record['attempt']} refused with {record['answeredWith']} (simulated eOCR outage)", **info))
        return
    closed_already = []

    def received(b):
        if b["state"] == "CLOSED":
            closed_already.append(True)
            add_event(b, "callback-duplicate", f"callback attempt {record['attempt']} ({st.get('code')} {st.get('value')}) arrived after the execution "
                      f"was closed by {b.get('closedBy')}; recorded, not processed again", **info)
            return
        b["state"] = "CALLBACK_RECEIVED"
        b["callback"] = {**info, "receivedAt": record["receivedAt"], "contractErrors": record["contractErrors"], "payload": p}
        add_event(b, "callback", f"callback accepted: {st.get('code')} {st.get('value')} - {st.get('description')}", **info)
        for e in record["contractErrors"]:
            add_event(b, "contract-error", e)

    batch = update_batch(cid, received)
    if closed_already:
        return
    result = retrieve_result(batch, st.get("code"), p.get("batchPath"))
    meta = {}
    try:  # eOCR validates the callback against the Status API before closing
        meta = {}
        code, body = call("GET", f"/integration/loan/status/{job_id}", base=batch.get("aceUrl"), meta=meta, job=cid)
    except Exception as exc:
        code, body = None, f"{type(exc).__name__}: {exc}"

    def close(b):
        if isinstance(body, dict) and code == 200:
            record_status(b, body, "callback reconciliation", meta.get("contentType"))
            diff = [f for f in ("status", "batchPath", "failedDocuments") if body.get(f) != p.get(f)]
            if diff:
                msg = "callback differs from Status API in " + ", ".join(f"{f} (callback {p.get(f)!r}, Status API {body.get(f)!r})" for f in diff)
                if msg not in b["contractErrors"]:
                    b["contractErrors"].append(msg)
                add_event(b, "contract-error", msg, statusApi={f: body.get(f) for f in diff})
        else:
            add_event(b, "warning", f"could not reconcile the callback with the Status API: HTTP {code}", body=body)
        b["result"] = result
        if result["key"]:
            add_event(b, "result", f"Response.json {'retrieved and matches spec 6.4/6.5' if result['ok'] else 'has contract errors'}: {result['key']}",
                      errors=result["errors"], **{k: v for k, v in (result["summary"] or {}).items() if k != "rows"})
        b.update(state="CLOSED", closedAt=now_iso(), closedBy="callback",
                 outcome={"code": st.get("code"), "value": st.get("value"), "description": st.get("description"),
                          "batchPath": p.get("batchPath"), "failedDocuments": p.get("failedDocuments")})
        _validate(b)
        _judge(b)
        add_event(b, "closed", f"execution closed on callback: {st.get('code')} {st.get('value')}")

    update_batch(cid, close)
    store_outcome(cid)


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
        if not isinstance(payload, dict):
            return self._reply(400, {"error": "body is not a JSON object"})
        base = f"{SIM_PREFIX}callbacks/{job_id}/"
        refuse = 0
        try:
            refuse = int(store.get_json(f"{SIM_PREFIX}flaky/{job_id}")[0]["refuse"])
        except KeyError:
            pass
        for _ in range(20):  # attempt numbers are claimed with a create-only write, so concurrent deliveries never share one
            attempts = len(store.get().list(base)) + 1
            answered = 503 if attempts <= refuse else 200
            record = {
                "receivedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "attempt": attempts,
                "answeredWith": answered,
                "callerPrincipal": principal.group(1) if principal else None,
                "idempotencyKey": self.headers.get("Idempotency-Key"),
                "headers": {k: v for k, v in self.headers.items() if k.lower() not in ("authorization", "x-amz-security-token", "cookie")},
                "contractErrors": check_callback(payload) + json_content_type(self.headers.get("Content-Type"), "callback"),
                "payload": payload,
            }
            try:
                store.put_json(f"{base}{attempts:03d}.json", record, if_none_match=True)
                break
            except Exception as exc:
                if not _precondition_failed(exc):
                    raise
        log("callback received", aceJobId=job_id, attempt=attempts, answered=answered,
            code=(payload.get("status") or {}).get("code"), contractErrors=len(record["contractErrors"]), caller=record["callerPrincipal"])
        threading.Thread(target=_trace_callback, args=(job_id, record), daemon=True).start()
        if answered == 503:
            return self._reply(503, {"error": "simulated intermittent eOCR outage"})
        self._reply(200, {"received": True})


def _trace_callback(job_id, record):
    try:
        on_callback(job_id, record)
    except Exception:
        log("callback tracing failed", aceJobId=job_id, trace=traceback.format_exc()[-1500:])


def serve():
    if CONSOLE_PORT:
        sys.modules.setdefault("eocr_sim", sys.modules[__name__])  # console shares this module, not a second copy
        import console  # the operator console: same process, its own port, never behind the Lattice target group
        console.start(CONSOLE_HOST, CONSOLE_PORT)
    store.get()  # connect (and create the simulator's tables) before accepting callbacks
    log("eOCR simulator callback receiver listening", port=PORT, bucket=INTAKE_BUCKET, store=store.MODE)
    ThreadingHTTPServer(("0.0.0.0", PORT), CallbackHandler).serve_forever()


# ------------------------------------------------------------------ ACE API client (SigV4 over VPC Lattice)

EXCHANGE_PREFIX = f"{SIM_PREFIX}exchanges/"
EXCHANGE_KEEP = 400
SECRET_HEADERS = {"authorization", "x-amz-security-token", "cookie", "set-cookie"}
BODY_KEEP = 16384


def _headers(h):
    return {k: ("(redacted)" if k.lower() in SECRET_HEADERS else v) for k, v in dict(h or {}).items()}


def _clip(text):
    text = text if isinstance(text, str) else (text or b"").decode(errors="replace")
    return text if len(text) <= BODY_KEEP else text[:BODY_KEEP] + f"… ({len(text) - BODY_KEEP} more characters)"


def record_exchange(cid, entry):
    """Append one ACE call to the job's exchange log. Identical repeats (a status poll that changed nothing) are
    folded into the previous entry with a count, so hours of tracking stay readable."""
    key = f"{EXCHANGE_PREFIX}{cid}.json"
    for attempt in range(8):
        try:
            log_, etag = store.get_json(key)
        except KeyError:
            log_, etag = {"correlationId": cid, "exchanges": []}, None
        items = log_["exchanges"]
        last = items[-1] if items else None
        norm = lambda e: (e["method"], e["url"], e["status"], re.sub(r'"timestamp":\s*"[^"]*"', "", e.get("responseBody") or ""))  # noqa: E731
        if last and norm(last) == norm(entry):
            last["repeats"] = last.get("repeats", 1) + 1
            last["lastAt"] = entry["at"]
        else:
            items.append(entry)
            del items[:-EXCHANGE_KEEP]
        try:
            store.put_json(key, log_, **({"if_match": etag} if etag else {"if_none_match": True}))
            return
        except Exception as exc:
            if not _precondition_failed(exc):
                raise
            time.sleep(0.05 * (attempt + 1))


def exchanges(cid):
    try:
        return store.get_json(f"{EXCHANGE_PREFIX}{cid}.json")[0]["exchanges"]
    except KeyError:
        return []


def call(method, path, body=None, expect=None, base=None, meta=None, job=None):
    """base: the ACE integration endpoint to call (default ACE_URL_INTEGRATION). meta, a dict, receives the response's contentType.
    job: the correlationId whose exchange log records this call (request, response, headers, timing)."""
    url = (base or INTEGRATION_URL).rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json", "x-amz-content-sha256": "UNSIGNED-PAYLOAD"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = AWSRequest(method=method, url=url, data=data, headers=headers)
    SigV4Auth(_session.get_credentials().get_frozen_credentials(), "vpc-lattice-svcs", REGION).add_auth(req)
    http = urllib.request.Request(url, data=data, method=method, headers=dict(req.headers.items()))
    started, at = time.time(), now_iso()
    code, text, ctype, rheaders, error = None, "", None, {}, None
    try:
        with urllib.request.urlopen(http, timeout=60) as r:
            code, text, ctype, rheaders = r.status, r.read().decode(), r.headers.get("Content-Type"), dict(r.headers.items())
    except urllib.error.HTTPError as e:
        code, text, ctype, rheaders = e.code, e.read().decode(errors="replace"), e.headers.get("Content-Type"), dict(e.headers.items())
    except Exception as e:
        error = f"{type(e).__name__}: {e}"
    finally:
        if job:
            try:
                record_exchange(job, {"at": at, "method": method, "url": url, "status": code if error is None else "error", "error": error,
                                      "ms": round((time.time() - started) * 1000), "requestHeaders": _headers(req.headers.items()),
                                      "requestBody": _clip(data) if data else None, "responseHeaders": _headers(rheaders),
                                      "responseBody": _clip(text) if text else None})
            except Exception as exc:
                log("exchange log failed", correlationId=job, error=f"{type(exc).__name__}: {exc}")
    if error:
        raise ConnectionError(f"{method} {url}: {error}")
    if meta is not None:
        meta["contentType"] = ctype
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

    def trace(self, change):
        """Mirror the run into the batch ledger so it shows in the console; never fails the scenario."""
        try:
            update_batch(self.correlation_id, change)
        except Exception as exc:
            log("ledger update failed", scenario=self.name, error=f"{type(exc).__name__}: {exc}")

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
        try:
            create_batch(self.loan_id, self.correlation_id, self.loan_info, self.extraction_required, source=f"scenario:{self.name}", track=False)
        except Exception as exc:
            log("ledger create failed", scenario=self.name, error=f"{type(exc).__name__}: {exc}")

        def staged(b):
            b.update(state="STAGED", controlFile=control, controlOverride=control if self.control_override else None,
                     documents=[{"fileName": n, "contentType": ct, "bytes": len(d), "uploadedAt": now_iso()} for n, ct, d in self.docs])
            add_event(b, "staged", f"{len(self.docs)} documents + controlfile.json written to s3://{INTAKE_BUCKET}/{self.folder}")
        self.trace(staged)
        self.ok(f"staged s3://{INTAKE_BUCKET}/{self.folder} ({len(self.docs)} documents + controlfile.json); batchPath sent: {self.batch_path}")

    def onboard(self):
        body = {"loanId": self.loan_id, "correlationId": self.correlation_id, "batchPath": self.batch_path}
        code, resp = call("POST", "/integration/loan/onboarding", body, expect=202, job=self.correlation_id)
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
            store.put_json(f"{SIM_PREFIX}flaky/{self.job}", {"refuse": self.flaky})
            self.ok(f"eOCR callback endpoint will refuse the first {self.flaky} deliveries with 503")
        index_job(self.job, self.correlation_id)

        def submitted(b):
            b.update(state="SUBMITTED", aceJobId=self.job, batchPath=self.batch_path, flaky=self.flaky, submittedAt=now_iso(), aceUrl=INTEGRATION_URL,
                     submittedBy=operator())
            add_event(b, "submitted", f"onboarding -> HTTP 202, aceJobId {self.job}", request=body, response=resp)
        self.trace(submitted)
        try:
            store_submission(self.correlation_id, body, code, resp)
        except Exception as exc:
            log("submission record failed", scenario=self.name, error=f"{type(exc).__name__}: {exc}")
        # idempotency: the same request again returns the same job
        _, again = call("POST", "/integration/loan/onboarding", body, expect=202, job=self.correlation_id)
        if again.get("aceJobId") != self.job:
            self.err(f"repeated onboarding returned a different job {again.get('aceJobId')}")
        else:
            self.ok("repeated onboarding request -> same aceJobId (idempotent)")

    def follow(self, timeout_s):
        deadline = time.time() + timeout_s
        last = None
        while time.time() < deadline:
            _, body = call("GET", f"/integration/loan/status/{self.job}", expect=200, job=self.correlation_id)
            errs = check_status_response(body, self.job)
            for e in errs:
                if e not in self.result["errors"]:
                    self.err(e)
            key = (body["workflow"].get("stage"), body["workflow"].get("state"), body["status"]["code"])
            if key != last or body["status"]["code"] in TERMINAL:
                self.trace(lambda b: record_status(b, body, f"scenario {self.name}"))
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
        _, review = call("POST", "/validate/reviewValidation", {"clientLoanNumber": self.loan_id, "adr": self.job}, expect=200, job=self.correlation_id)
        fields = [{"fieldName": f["fieldName"], "metadataValue": f.get("metadataValue"), "extractedValue": f.get("extractedValue"),
                   "isMatched": f["fieldName"] not in self.hitl_reject} for f in review["fieldDetails"]]
        self.ok(f"HITL review shown to reviewer: totalMatches={review['totalMatches']}, fields={[(f['fieldName'], f.get('isMatched')) for f in review['fieldDetails']]}")
        code, resp = call("POST", "/validate/updateValidation", {"clientLoanNumber": self.loan_id, "adr": self.job, "updatedFields": fields}, job=self.correlation_id)
        self.ok(f"HITL reviewer confirmed mismatches {self.hitl_reject} -> HTTP {code}")
        self.trace(lambda b: b.update(hitl={"loadedAt": now_iso(), "loadedBy": operator(), "fields": review["fieldDetails"],
                                            "totalMatches": review.get("totalMatches"),
                                            "decision": {"at": now_iso(), "by": operator(), "httpStatus": code,
                                                         "fields": [{"fieldName": f["fieldName"], "isMatched": f["isMatched"]} for f in fields]}}))
        self.hitl_reject = []

    def callbacks(self, timeout_s=900):
        deadline = time.time() + timeout_s
        prefix = f"{SIM_PREFIX}callbacks/{self.job}/"
        while time.time() < deadline:
            objs = sorted(o.key for o in store.get().list(prefix))
            records = [store.get_json(k)[0] for k in objs]
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
        if not re.search(r":assumed-role/[\w-]+-integration-task-role/", cb["callerPrincipal"] or ""):
            self.err(f"callback was not signed by the integration task role: {cb['callerPrincipal']}")
        else:
            self.ok(f"callback authenticated by IAM (Lattice) as {cb['callerPrincipal'].split('/')[-2]}")
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
            summary = {k: v for k, v in response_summary(doc).items() if k != "rows"} | {"sample": next(iter(doc["Documents"].items()))}
            self.result["responseFile"] = {"key": f"s3://{OUTPUT_BUCKET}/{p['batchPath']}", **summary}
            if not errs:
                self.ok(f"Response.json at s3://{OUTPUT_BUCKET}/{p['batchPath']} matches spec 6.4/6.5: "
                        f"{summary['documentTypes']} document types, {summary['documents']} documents, {summary['extractedFields']} extracted fields, validationStatus={doc['validationStatus']}")

    def run(self, timeout_s):
        OPERATOR.name = f"scenario runner ({os.environ.get('SIM_RUN_BY', 'eocr_sim.py run')})"
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
        self.trace(lambda b: add_event(b, "scenario", f"scenario {self.name} {'PASSED' if self.result['passed'] else 'FAILED'}",
                                       errors=self.result["errors"], checks=self.result["checks"]))
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

    total = len(PdfReader(io.BytesIO(pkg)).pages)

    def around_note(min_pages, max_pages):
        count = min(rnd.randint(min_pages, max_pages), total)
        # the slice holds the NOTE and stays inside the document
        first = rnd.randint(max(1, note_last - count + 1), max(1, min(note_first, total - count + 1)))
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
    store.put_json(key, report)
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
