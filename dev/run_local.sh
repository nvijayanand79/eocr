#!/usr/bin/env bash
# Runs the simulator console against moto (S3) and dev/fake_ace.py on this machine - no AWS account needed.
#   pip install -r requirements-aws.txt "moto[server]"
#   dev/run_local.sh            then open http://127.0.0.1:8081
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1
export AWS_ENDPOINT_URL=http://127.0.0.1:5055
export ACE_BUCKET_INTAKE=local-intake ACE_BUCKET_OUTPUT=local-output ACE_BUCKET_CONFIG=local-config
export ACE_URL_INTEGRATION=http://127.0.0.1:9000
export SIM_TRACK_INTERVAL_SECONDS=${SIM_TRACK_INTERVAL_SECONDS:-3} SIM_CALLBACK_GRACE_SECONDS=${SIM_CALLBACK_GRACE_SECONDS:-60}

pids=()
trap 'kill "${pids[@]}" 2>/dev/null' EXIT
moto_server -H 127.0.0.1 -p 5055 >/dev/null 2>&1 & pids+=($!)
for _ in $(seq 50); do curl -s -o /dev/null "$AWS_ENDPOINT_URL" && break; sleep 0.2; done
python3 dev/seed_local.py
python3 dev/fake_ace.py & pids+=($!)
python3 eocr_sim.py serve & pids+=($!)
echo "console: http://127.0.0.1:${SIM_CONSOLE_PORT:-8081}   (Ctrl-C to stop)"
wait
