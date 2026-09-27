"""End-to-end check of the eOCR lifecycle in QA.

eOCR side: the deployed simulator console (CloudFront /eocr-sim), signed in as the QA test user.
ACE side: the loan's reviewer, signed in to ACE (auth-service) with the same QA test user, acting only through
ACE's review API (/document/review/{adr}) - exactly what the review screen calls.

python lifecycle_check.py <scenario>
  happy       extraction requested, NOTE matches      -> reviews, NOTE PASSED, extraction review -> 0
  noextract   extraction not requested (T4)            -> review, NOTE PASSED                      -> 0
  validation  NOTE mismatch, reviewer confirms (T3)    -> review, NOTE review FAIL                 -> 2000
  notepass    NOTE mismatch, reviewer overrules        -> review, NOTE review PASS                 -> 0 (no extraction)
  precheck    locked + corrupt files (T1)              -> no review                                -> 1000
  resume      python lifecycle_check.py resume <correlationId> <ADR> <scenario>  (continue a waiting job)

Environment: ACE_URL (default the QA URL), ACE_TEST_USER and ACE_TEST_PASSWORD (an ACE account used for the
simulator console and the ACE review API; never printed). Use an account nobody else is signed in with:
ACE keeps one session per user."""
import base64, os, collections, http.cookiejar, json, random, sys, time, urllib.error, urllib.request

USER, PW = os.environ.get("ACE_TEST_USER", ""), os.environ.get("ACE_TEST_PASSWORD", "")
if not USER or not PW:
    sys.exit("set ACE_TEST_USER and ACE_TEST_PASSWORD")
HOST = os.environ.get("ACE_URL", "https://d237i8q2c495sn.cloudfront.net").rstrip("/")
B = HOST + "/eocr-sim"
jar = http.cookiejar.CookieJar()
op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))


def http(method, url, body=None, headers=None):
    h = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body is not None else None, method=method, headers=h)
    try:
        with op.open(req, timeout=120) as r:
            raw = r.read() or b"{}"
            return r.status, json.loads(raw)
    except urllib.error.HTTPError as e:
        raw = e.read() or b"{}"
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw[:300].decode(errors="replace")}


def api(method, path, body=None):
    return http(method, B + path, body)


def say(*a):
    print(time.strftime("%H:%M:%S"), *a, flush=True)


def ace_token():
    st, r = http("POST", HOST + "/auth-service/auth", {"username": USER, "password": base64.b64encode(PW.encode()).decode()})

    def find(o):
        if isinstance(o, str) and o.count(".") == 2 and o.startswith("eyJ"):
            return o
        if isinstance(o, dict):
            for v in o.values():
                t = find(v)
                if t:
                    return t
        return None
    tok = find(r)
    if not tok:
        sys.exit(f"ACE sign-in failed: HTTP {st}")
    return tok


def ace(method, path, body=None):
    return http(method, HOST + "/document/review/" + path, body, {"Authorization": TOKEN})


scenario = sys.argv[1] if len(sys.argv) > 1 else "happy"
st, me = api("POST", "/api/login", {"login": USER, "password": PW})
say("eOCR console sign-in", st, me.get("user"))
TOKEN = ace_token()
say("ACE sign-in ok (reviewer", USER + ")")

RESUME = scenario == "resume"
if RESUME:  # python lifecycle_check.py resume <CID> <ADR> <scenario>
    cid, adr, scenario = sys.argv[2], sys.argv[3], sys.argv[4]
    say("resuming", cid, adr, scenario)
else:
    tl = api("GET", "/api/testloan")[1]
    info = dict(tl["loanInfo"])
    if scenario in ("validation", "notepass"):
        info.update(loanAmount="1.00", sellerLoanNumber="0000000000")
    extraction = scenario == "happy"
    st, b = api("POST", "/api/batches", {"loanId": tl["loanId"], "loanInfo": info, "extractionRequired": extraction})
    cid = b["correlationId"]
    say("job created", st, cid, "extractionRequired", extraction)
    if scenario == "precheck":
        for kind in ("package", "locked", "corrupt"):
            say("document", kind, api("POST", f"/api/batches/{cid}/testdata", {"kind": kind, "pages": 30})[0])
    else:
        say("document", api("POST", f"/api/batches/{cid}/testdata", {"kind": "package", "pages": random.randint(40, 90)})[0])
    say("control file", api("PUT", f"/api/batches/{cid}/control", {})[0])
    say("staged", api("POST", f"/api/batches/{cid}/stage")[0])
    st, r = api("POST", f"/api/batches/{cid}/submit", {"batchPathMode": "file", "flaky": 0})
    adr = r.get("aceJobId")
    say("submitted", st, r.get("error", ""), "ADR", adr)

t0 = time.time()
last_status = last_ace = None
reviews = []
for _ in range(360):
    st, b = api("GET", f"/api/batches/{cid}")
    wf = (b.get("status") or {}).get("workflow") or {}
    key = (b.get("state"), wf.get("stage"), wf.get("state"), (b.get("status") or {}).get("status", {}).get("code") if isinstance((b.get("status") or {}).get("status"), dict) else None)
    if key != last_status:
        say("eOCR Status API:", *key)
        last_status = key
    if adr:
        st, v = ace("GET", adr)
        akey = (v.get("stage"), v.get("state"), v.get("reviewStage"), v.get("outcome"))
        if akey != last_ace:
            say("ACE lifecycle:", "stage", akey[0], akey[1], "| review", akey[2], "| outcome", akey[3])
            last_ace = akey
        rs = v.get("reviewStage")
        if rs in ("CLASSIFICATION", "COLLATION", "EXTRACTION"):
            st, r = ace("POST", adr + "/complete", {"comment": f"QA lifecycle test ({scenario})"})
            say(f"REVIEWER completes the {rs} review -> HTTP {st}", r.get("statusMessage", r.get("completed")))
            reviews.append(rs)
        elif rs == "COMPLETENESS":
            st, cmp_ = ace("GET", adr + "/note")
            fields = cmp_.get("fieldDetails") or []
            say("REVIEWER opens the NOTE review:", [(f.get("fieldName"), f.get("metadataValue"), f.get("extractedValue"), f.get("isMatched")) for f in fields])
            wrong = ("sellerLoanNumber", "loanAmount") if scenario == "validation" else ()
            decision = [{"fieldName": f["fieldName"], "isMatched": f["fieldName"] not in wrong} for f in fields]
            st, r = ace("POST", adr + "/note", {"fields": decision, "comment": f"QA lifecycle test ({scenario})"})
            say(f"REVIEWER sends the NOTE decision -> HTTP {st}", r.get("decision"), r.get("failures") or r.get("statusMessage", ""))
            reviews.append("NOTE:" + str(r.get("decision")))
    if b.get("state") == "CLOSED":
        break
    time.sleep(15)

o = b.get("outcome") or {}
say("CLOSED" if b.get("state") == "CLOSED" else "NOT CLOSED", o.get("code"), o.get("value"), o.get("description"), f"after {int(time.time() - t0)} s")
say("reviews done:", reviews)
say("callback attempts", (b.get("callback") or {}).get("attempt"), "| result ok", (b.get("result") or {}).get("ok"), "| errors", (b.get("result") or {}).get("errors"))
v = b.get("validation") or {}
say("validation", v.get("validationStatus"), "findings", v.get("findings"))
chk = b.get("specChecklist") or []
say("spec checklist", dict(collections.Counter(c.get("result") for c in chk)), [(c.get("clause"), c.get("errors")) for c in chk if c.get("result") == "failed"])
print("CID", cid, "ADR", adr)
