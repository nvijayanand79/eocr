"""Creates the local buckets and a synthetic test loan (loan.json + a 260-page PDF (room for every scenario slice around the NOTE)) for dev/run_local.sh."""
import io
import json
import os

import boto3
from pypdf import PdfWriter

s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"))
for bucket in (os.environ["ACE_BUCKET_INTAKE"], os.environ["ACE_BUCKET_OUTPUT"], os.environ["ACE_BUCKET_CONFIG"]):
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

w = PdfWriter()
for _ in range(260):
    w.add_blank_page(width=612, height=792)
buf = io.BytesIO()
w.write(buf)
prefix = os.environ.get("SIM_TESTDATA_PREFIX", "test-data/eocr/")
s3.put_object(Bucket=os.environ["ACE_BUCKET_CONFIG"], Key=prefix + "local-loan.pdf", Body=buf.getvalue())
loan = {"loanId": "1000123456", "file": "local-loan.pdf", "notePages": [60, 64],
        "loanInfo": {"sellerLoanNumber": "7700112233", "loanAmount": "250000.00", "borrowerLastName": "RIVERA"}}
s3.put_object(Bucket=os.environ["ACE_BUCKET_CONFIG"], Key=prefix + "loan.json", Body=json.dumps(loan).encode())
print("seeded local buckets and test loan", loan["loanId"])
