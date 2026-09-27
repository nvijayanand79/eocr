# eOCR Simulator — User Manual

For testers and developers who use the eOCR simulator to send loan packages to ACE the way eOCR does, follow them
through ACE, and check what ACE sends back. Screenshots are from the QA environment, 27 September 2026.

---

## 1. What the simulator is

eOCR is the client system that sends loan packages to ACE and receives the results (ACE to eOCR Integration
Specification v1.1). In QA the real eOCR is not connected, so the **eOCR simulator** plays its part:

| The simulator does (as eOCR would) | ACE does (for real) |
|---|---|
| Puts the loan documents and the control file in the intake S3 folder | Checks the package (pre-check), merges it into one PDF |
| Calls ACE's onboarding API and gets the ACE job ID (the ADR) | OCR, classification, collation, NOTE validation, extraction |
| Asks ACE's Status API for progress | Waits for a person to review the loan in ACE |
| Receives ACE's callback and fetches the result file | Sends exactly one callback when the job ends |
| Checks every answer against the specification and shows a verdict | — |

Nothing the console shows about progress is invented: it only records and checks what ACE reported through its
Status API, its callback and its result file.

**Where it is:** `https://d237i8q2c495sn.cloudfront.net/eocr-sim/` (QA). It is an internal test tool; only
people with an ACE account can sign in.

**Important:** reviews are done **in ACE**, not in the simulator. A job that needs a review waits until a reviewer
completes it in the ACE application (section 5).

---

## 2. Signing in

Open the simulator address and sign in with your **ACE user ID (or e-mail) and ACE password**. The simulator keeps
its own session: signing in here does not sign you out of ACE.

![Sign-in screen](img/01-sign-in.jpg)

Every job records who created, submitted and closed it, so use your own account.

**To review loans in ACE** (section 5) your ACE account needs a reviewer role (PROCESSOR, SUPERVISOR or ADMIN). An
account without any role cannot open the review screen.

---

## 3. The console at a glance

### 3.1 Navigation

The left bar has: **New job**, **Dashboard**, **Jobs**, **Callbacks**, **Report**, **System health**,
**Scenario runs** and **How it works** (help). At the bottom it shows the ACE endpoint jobs are sent to, and who is
signed in.

### 3.2 Dashboard

The dashboard is the starting page. The tiles count jobs submitted in the last 24 hours, jobs in progress in ACE,
completed and failed jobs, the average time until ACE's callback, jobs that need your attention, and tests passed.
Click a tile to see those jobs. Below are **Needs attention** (with the reason for each job), **In progress** and
**Recently finished**. Each row has a five-part progress bar: prepared, submitted, in ACE, callback, closed.

![Dashboard](img/02-dashboard.jpg)

### 3.3 Jobs

Every job, newest first, with its status, progress, where it is now, who submitted it and when, and how long it took.
Use the filters (needs attention, in progress, drafts, completed, failed, finished), the search box (loan ID, job ID or
ADR), the period and **Only mine**. **Export CSV** downloads the list.

![Jobs list](img/03-jobs.jpg)

The **Failed** filter shows each failure with ACE's reason in plain words:

![Jobs list filtered to failed jobs](img/03b-jobs-failed-filter.jpg)

---

## 4. Running a job, step by step

This walk-through is a real QA run: the test **Loan data differs from the NOTE**, which ended as ADR-2026-200000000031.

### Step 0 — Choose what to test

Click **New job**. The first screen asks *What do you want to test?* Each test prepares its package and states the
result ACE should give (the full list is in section 9). Choose **Start from scratch** to use your own loan data and
files.

![Choose a test](img/40-new-job-pick-test.jpg)

### Step 1 — Loan details

These are the loan fields eOCR puts in the control file: loan ID, seller loan number, loan amount and borrower last
name. The job ID (correlationId) is generated if you leave it empty. **Extract fields from the whole package** sets
`extractionRequired`: off means ACE only classifies and validates the NOTE. **Fill from the environment's test loan**
copies a known good loan. For this test the loan amount (1.00) and seller loan number (0000000000) are deliberately
wrong.

![Loan details](img/41-new-job-loan-details.jpg)

Click **Continue**. The job is created now; from here your progress is saved at every step and a draft can be
reopened later from Jobs.

### Step 2 — Documents

Add the loan documents: drag PDF files onto the box or choose files. Each file is uploaded straight into the job's
S3 folder, with a progress bar. For testing you can add ready-made files: a slice of a real loan package that contains
the NOTE (choose the number of pages), a password-protected PDF, or a corrupt PDF. The side panel shows the job, the
S3 folder and its files.

![Documents](img/42-new-job-documents.jpg)

### Step 3 — Control file

The control file is generated from steps 1 and 2. You can edit every field in the form, add or remove loan fields and
documents, or edit the raw JSON (useful for negative tests). The specification checks (section 3.2 of the spec) and the
JSON preview update as you type. **Reset to the generated file** undoes your edits. After the job is submitted the
control file is locked, because ACE has read it.

![Control file](img/43-new-job-control-file.jpg)

### Step 4 — Check the S3 folder

This shows exactly what ACE will read: the objects in the job's S3 folder, each with its role, compared with the
control file. A green line says the folder matches. A warning lists anything missing or unlisted; continue anyway only
if that is the point of your test.

![Check the S3 folder](img/44-new-job-check-folder.jpg)

### Step 5 — Submit to ACE

Check the ACE endpoint, choose what `batchPath` points to (the control file or the folder), optionally make the eOCR
endpoint refuse the first callbacks (to test ACE's retries), and review the expected outcome of the test. The exact
request that will be sent is shown. Click **Submit to ACE**.

![Submit](img/45-new-job-submit.jpg)

### After submitting

The job page opens. ACE answered with the ACE job ID (the ADR, here ADR-2026-200000000031), and the simulator starts
following the job. The test card at the top lists each expected result; they fill in as the job progresses.

![Job just submitted](img/46-job-submitted.jpg)

While ACE works, the status panel says what it is doing, and **Where the job is** shows each eOCR step and each
stage ACE reports, with times:

![ACE processing the job](img/47-job-in-ace.jpg)

When ACE needs a person, the panel turns amber: **Waiting for a HITL review**. Nothing more happens until a reviewer
acts in ACE:

![Waiting for the ACE reviewer](img/48-job-waiting-for-ace-reviewer.jpg)

---

## 5. Reviewing the job in ACE

Every eOCR job is reviewed in ACE by the loan's assigned reviewer:

1. the **classification/collation review** (document types and pages) — always;
2. the **NOTE review** — only when the NOTE does not match the control file;
3. the **extraction review** — only when extraction was requested.

### 5.1 Find the loan

Sign in to ACE (`https://d237i8q2c495sn.cloudfront.net/`) and search the pipeline for the ADR shown in the simulator.
The stage column tells you which review it is waiting for.

![ACE pipeline](img/50-ace-pipeline-job.jpg)

### 5.2 Classification / collation review

Open the loan. The **eOCR job** panel at the top right says which review ACE is waiting for and whether extraction was
requested. Check the document types and page ranges as usual and save your changes. Then add a comment if you like
and click **Complete review**.

![Review panel](img/51-ace-review-panel-collation.jpg)

![Completing the review](img/52-ace-review-complete-click.jpg)

ACE moves the job on by itself: it extracts the NOTE and compares it with the control file.

![Review completed](img/53-ace-review-completed.jpg)

### 5.3 NOTE review

If at least two of the three loan fields match, ACE passes the NOTE by itself. Otherwise the job waits for the NOTE
review, and the panel shows each field: the control-file value, the value ACE read on the NOTE, and a **Matches** box.

![NOTE review](img/54-ace-note-review.jpg)

**Show the NOTE pages** moves the viewer to the NOTE so you can compare:

![NOTE pages](img/55-ace-note-pages.jpg)

Jobs waiting for a NOTE review can also be found in the pipeline with the filter
**NOTE validation – Awaiting review**:

![Pipeline](img/56-ace-pipeline-before-decision.jpg)

Tick **Matches** for each field that is correct on the NOTE, add a comment, and click **Send decision**. The button
tells you the result before you send it: **Pass** when at least two of three fields match, otherwise **Fail**.

![Sending the NOTE decision](img/57-ace-note-decision.jpg)

A **Fail** ends the job as VALIDATION_FAILED (2000). A **Pass** continues with extraction (if requested) or ends the
job as COMPLETED (0).

![NOTE decision recorded](img/58-ace-note-decided.jpg)

---

## 6. Reading the result

Back in the simulator, the job closes as soon as ACE's callback arrives. The test card shows **Test passed** or
**Test failed** with every check; an expected failure is marked **As expected**.

![Job closed](img/60-job-closed-note-failed.jpg)

The **Validation** tab compares every loan field: the control-file value, the value ACE found, the simulator's own
comparison, ACE's verdict and the reviewer's decision.

![Validation tab](img/61-job-closed-validation-tab.jpg)

The **Callbacks** tab shows the callback ACE sent: status, time, attempt, Idempotency-Key and the specification
checks.

**Files ACE did not process.** ACE leaves out a file that is in the S3 folder but not in the control file
(*NOT_LISTED*), and a file that is an exact copy of a file listed before it (*DUPLICATE*, with the name of the file it
copies). The callback and the Status API name these files in `ignoredDocuments`; the Overview, Result and Callbacks
tabs list them under **Files ACE did not process**, and show nothing when there are none. They are not failures: the
job still completes. Older ACE versions do not send the list.

![Callbacks tab](img/62-job-closed-callbacks-tab.jpg)

---

## 7. The job page in detail

Every job page has three parts: the **status panel** (what is happening or what went wrong, and what to do next,
with one-click actions), **Where the job is** (the stage track), and the tabs. The examples below are a completed job
with extraction (ADR-2026-200000000025).

| Tab | Shows |
|---|---|
| **Overview** | what was sent (loan data, documents, control file) and what came back (outcome, documents classified, fields extracted, validation, spec checks, and **Files ACE did not process** when there are any) |
| **Files in S3** | the input folder (what ACE read), ACE's output folder and the simulator's own records, with size, time, view and download |
| **Result** | the outcome and the result file (Response.json): every document ACE found with its type, pages and extracted fields |
| **Validation** | NOTE validation per field and the document checks |
| **Callbacks** | every callback delivery attempt for this job, with the files ACE did not process when the callback names any |
| **ACE exchanges** | every call to ACE (onboarding, status polls) with request, response and time |
| **Spec compliance** | the specification checklist: passed, failed (with the deviation), not applicable, pending |
| **Activity** | ACE's status history and the job's timeline (who did what, when) |

![Overview](img/11-tab-overview.jpg)

![Files in S3](img/11-tab-files.jpg)

![Result](img/11-tab-result.jpg)

![Validation](img/11-tab-validation.jpg)

![ACE exchanges](img/11-tab-exchanges.jpg)

![Spec compliance](img/11-tab-spec.jpg)

![Activity](img/11-tab-activity.jpg)

**Evidence** (top right of every job) downloads one zip with a summary, the control file, the records, the
callbacks, the exchange log and ACE's output — attach it to a defect or a sign-off.

---

## 8. When a job fails

The status panel says what failed, where, and how to fix it.

**Pre-check failed (1000).** ACE refused one or more documents before processing. Each document is listed with the
reason (for example *Password Protected*, *Corrupted Document*). Fix or replace the file and use **Resubmit as new
job**, which copies the package into a new job and opens it at the Documents step.

![Pre-check failed](img/20-job-precheck-failed.jpg)

**NOTE validation failed (2000).** The reviewer confirmed that loan data differs from the NOTE. The panel lists each
field with both values. Correct the loan data (or the NOTE) and resubmit.

![NOTE validation failed](img/21-job-note-failed.jpg)

![NOTE validation failed — validation tab](img/22-job-note-failed-validation.jpg)

| Outcome | Code | What it means | What to do |
|---|---|---|---|
| COMPLETED | 0 | ACE finished; the result file has the documents (and fields, if requested) | nothing |
| PRECHECK_FAILED | 1000 | a document or the control file was refused before processing | fix the named files, resubmit |
| VALIDATION_FAILED | 2000 | the NOTE does not match the control-file loan data (confirmed by the reviewer) | correct the data, resubmit |
| PROCESSING_FAILED | 3000 | ACE could not finish (a step failed after all retries) | resubmit; if it repeats, report it with the Evidence zip |
| Refused by ACE | — | ACE rejected the onboarding request itself | read the message on the job page, fix, **Fix and submit again** |

---

## 9. Tests you can run

| Test | What it does | ACE should end with |
|---|---|---|
| Happy path | a clean package with the NOTE, extraction requested | 0, NOTE PASSED, fields extracted |
| Classification only | `extractionRequired` false | 0, no extracted fields |
| Package in several files | the package split over two PDFs | 0, NOTE PASSED |
| eOCR endpoint down for 2 callbacks | the simulator refuses ACE's first two callbacks | 0, delivered on attempt 3 |
| Password-protected file | one file needs a password | 1000, *locked.pdf — Password Protected* |
| Corrupt file | one file is not a readable PDF | 1000, *broken.pdf — Corrupted* |
| Document missing from the folder | the control file lists a file that was never uploaded | 1000, *ghost.pdf* |
| Control file for another loan | the control file's loan ID differs | 1000, *Loan ID Mismatch* |
| Loan data differs from the NOTE | wrong loan amount and seller loan number | 2000 after the reviewer confirms the mismatch |
| Ignored files | the happy-path package, plus *not-in-control-file.pdf* in the folder but not in the control file, and an exact copy of the package listed under a second name (*…_copy.pdf*) | 0, NOTE PASSED, and both files under **Files ACE did not process**: *NOT_LISTED* and *DUPLICATE* of the package |

In the Ignored files test the **Check S3 folder** step warns that a file is not in the control file. That is the point
of the test: continue.

Every test except the pre-check ones waits for reviews in ACE (section 5) before it can finish.

---

## 10. Other pages

**Callbacks** — every callback the simulator received from ACE, for all jobs (including unknown job IDs), with the
answer given, the checks and the payload. Use the filters to find refused or off-spec deliveries.

![Callbacks](img/30-callbacks.jpg)

**Report** — a summary for sign-off over the last 1, 7, 30 or 90 days: jobs submitted, tests passed per test,
specification coverage per clause, time spent in each ACE stage, time to callback, outcomes. **JSON** downloads it.

![Report](img/31-report.jpg)

**System health** — checks that the callback endpoint, ACE's Status API, the job store, the S3 buckets and the
tracker work, with the configuration in use.

![System health](img/32-health.jpg)

**Scenario runs** — reports from automated runs of the whole test set.

![Scenario runs](img/33-scenario-runs.jpg)

**How it works** — a short explanation of the terms used in the console.

![Help](img/34-help.jpg)

---

## 11. Running tests from the command line

`tools/lifecycle_check.py` in the simulator repository runs a whole test, including the reviews in ACE, without the
browser:

```
set ACE_TEST_USER=<an ACE account with a reviewer role that nobody else is using>
set ACE_TEST_PASSWORD=<its password>
python tools/lifecycle_check.py happy        (or: noextract, validation, notepass, precheck)
python tools/lifecycle_check.py resume <jobId> <ADR> <test>
```

---

## 12. Troubleshooting

| Problem | Why | What to do |
|---|---|---|
| The job stays at *Waiting for a HITL review* | a review is waiting in ACE | open the ADR in ACE and complete the review (section 5) |
| ACE's review screen shows no panel or does not load | your ACE account has no role | ask an administrator to give the account a reviewer role |
| ACE signs you out, or actions answer *not authorised* | the same ACE account signed in somewhere else (ACE keeps one session per user) | use your own account; do not share test accounts between people or scripts |
| *No callback* flag on a job | ACE finished but the callback did not arrive in time | open the job and use **Close…**: it reads the outcome from ACE's Status API and fetches the result |
| *No change 24h+* flag | nothing has moved for a day | check whether a review is waiting in ACE; otherwise report it with the Evidence zip |
| Sign-in to the simulator fails | wrong ACE user ID/password, or the account is not Active | check the account in ACE |
| Submit is refused by ACE | the request or control file is not acceptable | read the message on the job page, fix, **Fix and submit again** |

---

## 13. Terms

| Term | Meaning |
|---|---|
| Job (correlationId) | one eOCR submission of a loan package |
| ADR (aceJobId) | ACE's reference for the job, e.g. ADR-2026-200000000031; the same number in ACE, the callback and the result |
| Control file | the JSON file eOCR puts next to the documents: loan data, `extractionRequired`, the list of documents |
| batchPath | where ACE should read the package: the control file or its folder |
| Callback | ACE's message to eOCR when the job has ended, with the outcome code |
| Response.json | the result file: documents found, pages, confidence and extracted fields |
| HITL | human in the loop: a review by a person in ACE |
| NOTE validation | comparing the loan data in the control file with the NOTE; two of three fields must match |
