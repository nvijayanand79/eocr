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

Scenarios (`run --only a,b`): `api-contract`, `happy-path` (3 source PDFs, extraction, NOTE validation
passes, eOCR refuses the first 2 callbacks), `no-extraction`, `validation-failed` (wrong loan amount and
seller loan number; the harness confirms the mismatches as the HITL reviewer), `precheck-failed`
(password-protected + corrupt PDF), `control-file-mismatch`.

Test data is read from `s3://<config>/test-data/eocr/`: `loan.json` (`{"loanId", "file", "loanInfo"}`) and
the package PDF it names.
