# eOCR simulator

Test harness that plays the **eOCR partner system** against the ACE integration service, following
*ACE to eOCR Integration Specification v1.1*. It is not part of the ACE product and can be removed from an
environment by deleting its `eocrsim` entry in `newrez-ace-infra/config/<env>.json`.

| Mode | What it does |
|------|--------------|
| `serve` (ECS service) | eOCR's callback endpoint `POST /eocr/callback` (spec 5). Checks every callback against the contract, records it in `s3://<intake>/eocr-sim/callbacks/<aceJobId>/NNN.json`, and can refuse the first N deliveries with 503 to simulate an intermittent eOCR outage. On the accepted callback it retrieves `*Response.json`, reconciles with the Status API and **closes** the execution. Also runs the **operator console** (below). |
| `run` (ECS run-task) | Scenario driver (spec 3-4, 6-7): stages `loanId=<id>/correlationId=<execution>/controlfile.json` + documents in the intake bucket, calls `POST /integration/loan/onboarding`, follows `GET /integration/loan/status/{aceJobId}`, waits for the callback and verifies `*Response.json` in the output bucket. Writes a JSON report to `s3://<intake>/eocr-sim/reports/`. |

Authentication is IAM only: the simulator's task role signs its calls to ACE (SigV4 over VPC Lattice), and
ACE signs its callbacks to the simulator the same way. There are no keys, tokens or passwords.

Scenarios (`run --only a,b`):

| Scenario | Package | Expected |
|---|---|---|
| `api-contract` | none | unknown aceJobId -> HTTP 200 + 4040; malformed batchPath -> 400 |
| `happy-path` | 120-159 NOTE-bearing pages split into 3 PDFs, extraction required; eOCR refuses the first 2 callbacks (503) | COMPLETED (0), NOTE validation PASSED, extracted fields, callback retried with the same Idempotency-Key |
| `no-extraction` | 40-69 pages, one PDF, `extractionRequired=false`, batchPath given as the folder | COMPLETED (0), extraction objects empty |
| `resubmission` | the same PDF as `no-extraction`, new correlationId, after it finishes | a new aceJobId, COMPLETED (0). (With ACE's duplicate detection switched on, integration would end it as PROCESSING_FAILED naming the original.) |
| `validation-failed` | 80-109 pages, wrong loan amount and seller loan number | HITL review (the harness confirms the mismatches as the reviewer) -> VALIDATION_FAILED (2000) with Response.json without extraction |
| `precheck-failed` | good PDF + password-protected PDF + corrupt PDF | PRECHECK_FAILED (1000) naming both bad files |
| `control-file-mismatch` | control file loanId differs from the request | PRECHECK_FAILED (1000) naming the control file |

Slice sizes are random per run and differ between scenarios, so no two packages are identical (ACE can mark a
package whose page count and OCR text match an earlier upload as a DUPLICATE when that check is enabled).

Test data is read from `s3://<config>/test-data/eocr/`: `loan.json`
(`{"loanId", "file", "notePages": [first, last], "loanInfo": {...}}`) and the package PDF it names.
## Operator console (initiate, track and close by hand)

`serve` also runs a browser console on port `8081` that lets a person act as eOCR for one loan at a time, with every
step traced end to end:

| Step | What the console does | Spec |
|---|---|---|
| 1. New execution | picks `loanId` + a fresh `correlationId` (eOCR execution); "Fill from test loan" copies `loan.json` | 2, 3.1 |
| 2. Documents | uploads files into `s3://<intake>/loanId=<id>/correlationId=<execution>/`; one click adds a test package slice around the NOTE, a password-protected PDF or a corrupt PDF | 3.1 |
| 3. Control file | form (loanInfo, `extractionRequired`) with live JSON preview, or hand-edited raw JSON for negative tests; "Write control file to S3" | 3.2 |
| 4. Onboarding | `POST /integration/loan/onboarding` with the control file or folder as `batchPath`; optionally refuse the first N callbacks (503) | 4 |
| 5. Tracking | polls `GET /integration/loan/status/{aceJobId}` for every open execution and records each stage/state change; HITL panel when `HITL_PENDING` (acts as the ACE reviewer) | 7 |
| 6. Callback | every delivery attempt: HTTP answer, caller principal, Idempotency-Key, contract errors | 5, 6 |
| 7. Close | on the accepted callback: fetch and check `*Response.json`, compare with the Status API, close. **Close...** reconciles by hand when no callback comes (Status API terminal -> result retrieved; otherwise ABANDONED). A late callback after that is recorded, not re-processed | 3.3, 6.4, 6.5, 7 |

"Resubmit as new execution" copies the package into a new `correlationId`.

Views, for coming back days later:

| Tab | Shows |
|---|---|
| **Pipeline** | every execution as a board: Draft, Staged, Processing in ACE, Awaiting callback, Closed completed, Closed failed, Rejected. Search by loanId / correlationId / aceJobId and pick a period (24h, 7d, 30d, all). Flags: *no callback* (Status API terminal but no callback after the grace period), *no change 24h+* for open executions, contract errors, closed by hand |
| **Executions** | one execution in full: stepper, documents, control file, onboarding, stage history, callbacks (every attempt with payload, headers and checks), result, contract checks, **stored records**, timeline |
| **Callbacks** | every delivery the eOCR endpoint received across all jobs, including unknown aceJobIds; filter by accepted / refused / contract errors / unknown; click a row for payload, headers, Idempotency-Key and checks |
| **Scenario reports** | `run` reports; scenario runs also write their executions into the ledger, so they are on the board too |

### ACE endpoint

The console's default ACE endpoint is `ACE_URL_INTEGRATION`, and it can be changed from the header ("change"); the new
default is saved in `s3://<intake>/eocr-sim/settings.json` and "Use environment default" goes back. Each submission can
name another endpoint. The execution stores the endpoint it was submitted to (also in its submission record), and the
tracker, HITL, close and callback reconciliation for that execution all use it. Set `SIM_ACE_URL_ALLOWED`
(comma-separated base URLs) to restrict which endpoints may be used: the simulator signs its calls with its task role.

### What is stored (all in the intake bucket, nothing on local disk)

| Key | Written | Holds |
|---|---|---|
| `eocr-sim/batches/<correlationId>.json` | on every step | the execution's live state and timeline |
| `eocr-sim/records/<correlationId>/submission-<time>.json` | on each onboarding call, never changed | ACE endpoint, request, HTTP status and response, the control file as ACE reads it, each document's S3 key, size and ETag |
| `eocr-sim/records/<correlationId>/outcome.json` | once, on close | outcome, accepted callback, every delivery attempt, Status API at close, stage history, Response.json checks, contract errors |
| `eocr-sim/records/<correlationId>/<name>Response.json` | once, on close | copy of ACE's `*Response.json`, so the result is still there after the output bucket's retention |
| `eocr-sim/callbacks/<aceJobId>/NNN.json` | per delivery | payload, headers, answer, checks |
| `eocr-sim/jobs/<aceJobId>.json`, `eocr-sim/active/`, `eocr-sim/settings.json` | | job -> execution index, executions being polled, console settings |

The console reads everything back from S3 (with in-memory caches keyed by ETag), so a restarted or replaced task shows
the same history.

Each execution is one JSON document, `s3://<intake>/eocr-sim/batches/<correlationId>.json` (state, control file,
documents, aceJobId, stage history, callback, result, outcome and a timeline of events). Writes use S3 conditional
requests (`If-Match`), so the console, the callback receiver and a separate `run` task never overwrite each other.
`eocr-sim/jobs/<aceJobId>.json` maps a job back to its execution and `eocr-sim/active/` lists executions being polled.

Lifecycle: `DRAFT -> STAGED -> SUBMITTED -> IN_PROGRESS -> CALLBACK_RECEIVED -> CLOSED` (`REJECTED` when onboarding is refused).

### Opening it

The console has no login of its own, so by default it binds to `127.0.0.1` inside the task and is not in the
Lattice target group. Open it through ECS Exec / SSM port forwarding (the service needs `enableExecuteCommand` and
the task role the SSM messages permissions):

```bash
aws ssm start-session --target "ecs:<cluster>_<taskId>_<containerRuntimeId>" \
  --document-name AWS-StartPortForwardingSession --parameters portNumber=8081,localPortNumber=8081
# then http://localhost:8081
```

The task role that `serve` uses needs what `run` already has: invoke the integration service (Lattice), read/write
the intake bucket, read the output bucket and the test data.

| Variable | Default | |
|---|---|---|
| `SIM_CONSOLE_PORT` | `8081` | `0` turns the console and its tracker off |
| `SIM_CONSOLE_HOST` | `127.0.0.1` | do not widen without putting authentication in front |
| `SIM_TRACK_INTERVAL_SECONDS` | `15` | Status API polling of open executions |
| `SIM_CALLBACK_GRACE_SECONDS` | `600` | after the Status API goes terminal, how long before "no callback" is flagged |
| `SIM_MAX_UPLOAD_MB` | `512` | per file |
| `SIM_ACE_URL_ALLOWED` | (any) | comma-separated ACE base URLs a submission or the default may use |

### Trying it locally (no AWS)

`dev/run_local.sh` starts moto as S3, seeds a synthetic test loan, and runs `dev/fake_ace.py`, a stand-in for
ACE's integration service (onboarding, stages, pre-check of the control file and PDFs, a HITL pause for a
suspicious loan amount / seller loan number, `*Response.json` and callbacks with retries), then `serve`:

```bash
pip install -r requirements-aws.txt "moto[server]"
dev/run_local.sh          # console on http://127.0.0.1:8081
```

With the same environment variables `python3 eocr_sim.py run` runs every scripted scenario against the fake ACE.
The fake is only for clicking through the console; it is not a model of ACE's real processing.
