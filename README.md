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
step traced end to end.

**Screens.** A side navigation leads to:

- **Dashboard**: tiles (submitted in 24 h, in progress, completed %, failed, average time to callback, needing
  attention; each opens the filtered job list), *Needs attention* with the reason for each job, *In progress* and
  *Recently finished*, each row with a five-segment progress bar (prepared, submitted, ACE, callback, closed).
- **Jobs**: every job with status, progress, where it is, who submitted it and when, and how long it took; filters
  (needs attention, in progress, drafts, completed, failed, finished), search, "only mine", paging and CSV export.
- **New job**: a full-page wizard. 1 *Loan details* (the control file's loanInfo, extraction on/off, fill from the
  test loan) · 2 *Documents* (drag and drop or pick files; each uploads straight into the job's S3 folder with a
  progress bar; test files: a loan package slice with the NOTE, a password-protected PDF, a corrupt PDF) ·
  3 *Control file* (generated from steps 1 and 2, then editable: a form for every loanInfo field (add your own,
  remove any), extractionRequired and the documents list (file name, content type, add/remove, match the uploads),
  or the raw JSON; spec 3.2 checks and a JSON preview update as you type; "Reset to the generated file" undoes the
  edits. The file stays editable until the job is submitted; after that ACE has read it and it is locked) ·
  4 *Check S3 folder* (the objects S3 actually holds, each with its role, against the control file) · 5 *Submit*
  (ACE endpoint, batchPath, simulated callback outage, the exact request). A side panel shows the job, the target S3
  folder and its files throughout; progress is saved at every step, and a draft reopens where it stopped.
- **Job page** (one per submission):
  - a status panel in plain words: what is happening, or what went wrong, *where* (the failed stage), each affected
    item with its problem and how to fix it (a rejected document and the reason; a loan field with the control-file
    value against the NOTE value; ACE's refusal), what to do next, and one-click actions (resubmit as a new job, open
    the HITL review, close from the Status API, copy a failure summary);
  - **Where the job is**: a stage track of the eOCR steps and every stage ACE reports, each with its time and
    duration, the current step highlighted and the failed one in red;
  - tabs: *Overview* (what was sent and what came back), *Files in S3* (the input folder, ACE's output folder and
    the simulator records, with role, size, time, view and download), *Result*, *Validation*, *Callbacks*,
    *Spec compliance*, *Activity*.
- **Callbacks** and **Scenario runs**.

"Resubmit as new job" copies the package into a new job and opens its wizard at *Documents*, so the failing files
can be replaced first. Files are viewed inline only for PDF, images, JSON and text, and every file response is
sandboxed, so nothing uploaded can run inside the console.

The steps and the spec sections behind them:

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

Everything the console shows about a job's progress comes from ACE itself: the Status API, ACE's callbacks and the
result file in ACE's output bucket. The console never makes up a status; it only records, checks and displays what
ACE reported. What it decides on its own is eOCR's side: draft/staged before submission, closing, and the warning flags.

Each execution opens on a **status banner** that says in words what is happening and what, if anything, you need to
do (e.g. "Action needed: HITL review", "ACE finished but has not called back"). It then shows two sub-tabs:
**Tracking** (progress in ACE, callbacks, result, spec compliance, stored records, timeline), the default once
submitted, and **Package & submission** (documents, control file, submit). Times are shown in your local time, with
UTC on hover. Sign-in (below) records who created, submitted and closed each execution, and the pipeline can show "only mine". The **?** button explains the terms.

Views, for coming back days later:

| Tab | Shows |
|---|---|
| **Pipeline** | summary tiles (executions, submitted in 24h, open, completed %, failed, average time to callback, need attention; click a tile to filter) over a board: Draft, Staged, Processing in ACE, Awaiting callback, Closed completed, Closed failed, Rejected. Search by loanId / correlationId / aceJobId and pick a period (24h, 7d, 30d, all). Flags: *no callback* (Status API terminal but no callback after the grace period), *no change 24h+* for open executions, contract errors, closed by hand. Empty columns are folded into one line. **Export CSV** downloads the executions shown |
| **Executions** | one execution in full: stepper, documents, control file, onboarding, stage history, callbacks (every attempt with payload, headers and checks), result, contract checks, **stored records**, timeline |
| **Callbacks** | every delivery the eOCR endpoint received across all jobs, including unknown aceJobIds; filter by accepted / refused / contract errors / unknown; click a row for payload, headers, Idempotency-Key and checks. Refreshes on its own; **Export CSV** downloads every delivery |
| Tab badges | Pipeline: executions needing attention. Callbacks: deliveries with spec deviations or for unknown jobs |
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

### Sign-in: who did what

Every action (create, upload, write control file, submit, HITL decision, close, resubmit) needs a signed-in user; the
server refuses anything else with 401. The user is stored on the execution (`createdBy`, `submittedBy` with the time,
`closedByUser`), in the submission and outcome records, and on every timeline event. Scenario runs are recorded as
`scenario runner (…)` (set `SIM_RUN_BY` to name the pipeline or person that started them).

| `SIM_AUTH` | How the user is known | Use when |
|---|---|---|
| `name` (default) | a sign-in screen asks for a name and keeps it in a cookie | the console is reached by SSM port-forward; it records who, it does **not** authenticate |
| `oidc` | the console sits behind an ALB with an OIDC listener rule (company SSO or Cognito). The ALB signs the user's claims into `x-amzn-oidc-data`; the console verifies the ES256 signature with the ALB's regional public key, the signer (`SIM_ALB_ARN`) and the expiry, and takes the user from `SIM_OIDC_USER_CLAIMS` (default `email,preferred_username,name,sub`). No sign-in screen and a forged header is rejected | people open the console in a browser; set `SIM_CONSOLE_HOST=0.0.0.0` and put the ALB in front (infra change) |

### Lists and paging

The executions list shows 50 per page with Prev/Next and filters on the server (search, state, "needs attention").
Board columns show 20 cards and "Show more". The callbacks tab shows 100 deliveries per page with server-side filters.
Every list item and card shows who submitted it and when, and how long it took.

### Validation view

Each execution's Tracking tab has a **Validation** section:

- **NOTE validation**: for every loan field in the control file (seller loan number, loan amount, borrower last name,
  and any other), the control-file value, the value ACE found (NOTE first, then other document types), the simulator's
  own comparison (numbers compared as amounts, text case-insensitively), ACE's verdict (from `validationStatus` and
  the mismatches its description names) and the HITL reviewer's decision. On VALIDATION_FAILED ACE returns no extracted
  values (spec 6.5), so the values come from the HITL review, which the console keeps once it has been loaded.
  When ACE stopped earlier (pre-check or processing failure) the section says validation did not run.
- **Document checks**: every submitted document, and any the control file lists but the folder lacks, with its
  pre-check result, the document types ACE classified it into, pages, extracted fields and duplicate pages.
- **Findings**, counted like spec deviations (needs attention, "N issues" chip): ACE passed validation although a NOTE
  value differs from the control file; ACE reports a mismatch for a field whose values are equal; VALIDATION_FAILED
  without naming a field; `validationStatus` FAILED on another outcome; a document that passed pre-check but is not in
  Response.json; `failedDocuments` naming a file that was never submitted.

The view is frozen on the execution and in `outcome.json` when it closes.

### Spec coverage (ACE to eOCR Integration Specification v1.1)

Each execution's **Spec compliance** card is a checklist of these clauses (passed / failed with the deviations / not
applicable / pending), and the same checklist is frozen into `outcome.json`.

| Spec | What the simulator does and checks |
|---|---|
| 3.1 Source S3 structure | stages `loanId=<loanId>/correlationId=<execution>/<control>.json` + documents; several executions per loan |
| 3.2 Control file | writes it; checks loanInfo has loanId, correlationId, sellerLoanNumber, loanAmount, borrowerLastName (non-empty, ids matching the folder), `extractionRequired` is the string `"true"`/`"false"`, every document has fileName + contentType, is in the folder with that content type, and nothing unlisted is in the folder |
| 3.3 Staging rules 1-7 | onboarding, async processing, status, result file, callback, then step 7: eOCR validates the callback and retrieves the result when batchPath is set |
| 4, 4.1 Onboarding | `POST /integration/loan/onboarding` with loanId, correlationId, batchPath (control file or folder); malformed requests in `api-contract` |
| 4.2 Acknowledgement | HTTP 202, `application/json`, exactly `{aceJobId, status {202, ACCEPTED, "Request accepted for processing."}}`; a 2xx other than 202 with an aceJobId is still tracked and reported |
| 5 Callback endpoint | `POST /eocr/callback`, `application/json`, aceJobId of a job eOCR submitted (unknown jobs are shown), terminal values only |
| 5.1, 5.2 Callback payload | exactly the five flat fields, numeric code matching value, non-empty description, ISO 8601 UTC timestamp (fractions and `+00:00` allowed), failedDocuments items `{documentName, reason}` |
| 6.1-6.3 Per-status payloads, status mapping | batchPath `<aceJobId>/<name>Response.json` for 0 and 2000, empty otherwise; failedDocuments populated only for 1000 |
| 6.4 *Response.json | one per batch, top-level fields and order, `extractionRequired` as sent, `validationStatus` PASSED/FAILED/NA and as the outcome implies, Documents items and their field order, fileName of a submitted document, pageRange syntax, confidencePercentage keyed by pages inside pageRange with 0-100 values, extraction `{Value, cr}`, fileSize a byte count |
| 6.5 VALIDATION_FAILED | same schema, validationStatus FAILED, extraction empty; plus the NOTE validation view (control file vs NOTE vs ACE vs HITL) |
| 7.1-7.4 Status API | `GET /integration/loan/status/{aceJobId}` with `Accept: application/json`; response `application/json`, exact fields, workflow stage/state, code/value, output rules, timestamp; 4040 with `workflow {}` for unknown jobs |
| 7 Status progression | never back from a terminal status, never a different terminal outcome, never 4040 for an accepted job |
| 5 vs 7 | callback status, batchPath and failedDocuments equal the Status API's |
| Consistency rule | status.code and status.value must match, everywhere |

Not checked, because the spec does not define it: the aceJobId format, authentication (IAM here), onboarding error
bodies, the separator in the top-level `fileName` ("concatenated"), what `fileSize` counts, the format of `cr`, and
whether pageRange stays within each PDF's page count.

### How problems are reported

| Problem | What the console shows |
|---|---|
| NOTE validation failed (HITL confirmed mismatches) | red banner "Failed: VALIDATION_FAILED" with ACE's description (e.g. "Loan Amount Mismatch, Seller Loan Number Mismatch."), Response.json with `validationStatus FAILED` and no extraction |
| Pre-check failed (password-protected, corrupt, missing file, control-file loanId mismatch) | red banner with each failed document and reason; a document the control file lists but the folder lacks is also warned about before you submit |
| Processing failed (3000) | red banner with ACE's description |
| ACE refused the submission | REJECTED, banner with ACE's HTTP status and message, submission record with the response |
| HITL review waiting | amber "Action needed" banner with a button to the review; flagged *needs attention* |
| ACE finished but never called back | *no callback* flag after `SIM_CALLBACK_GRACE_SECONDS`; Close… reconciles from the Status API and retrieves the result |
| No progress for 24h | *no change 24h+* flag |
| Callback breaks the spec (fields, code type, code/value pair, output rules, timestamp) | "ACE sent an invalid callback" banner, each deviation listed, callbacks badge |
| Callback disagrees with the Status API | spec deviation naming both values |
| ACE passes validation although the NOTE disagrees with the control file | Validation finding, "Completed, with problems" |
| A submitted document is missing from the result | Validation finding naming the document |
| Result file missing or off-spec | "Completed, with problems" banner and list chip, each deviation listed |

Everything flagged counts in the *need attention* tile, the Pipeline badge and the "Needs attention" list filter.
`dev/fake_ace.py` can produce each of these on purpose (see its docstring) so the reporting can be checked locally.

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
| `SIM_AUTH` | `name` | `name` (sign-in screen) or `oidc` (ALB single sign-on, verified) |
| `SIM_ALB_ARN` | | with `oidc`: only tokens signed by this ALB are accepted |
| `SIM_OIDC_USER_CLAIMS` | `email,preferred_username,name,sub` | with `oidc`: which claim names the user |

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
