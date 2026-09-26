# eOCR simulator

Test harness that plays the **eOCR partner system** against the ACE integration service, following
*ACE to eOCR Integration Specification v1.1*. It is not part of the ACE product and can be removed from an
environment by deleting its `eocrsim` entry in `newrez-ace-infra/config/<env>.json`.

| Mode | What it does |
|------|--------------|
| `serve` (ECS service) | eOCR's callback endpoint `POST /eocr/callback` (spec 5). Checks every callback against the contract, records it in `s3://<intake>/eocr-sim/callbacks/<aceJobId>/NNN.json`, and can refuse the first N deliveries with 503 to simulate an intermittent eOCR outage. |
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