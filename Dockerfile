# eOCR simulator (test harness). Built by the environment's CodeBuild project like every ACE image.
ARG AWS_ACCOUNT_ID
ARG AWS_REGION
ARG BASE_IMAGES_REPO_NAME
ARG PYTHON_IMAGE_TAG
FROM ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${BASE_IMAGES_REPO_NAME}:${PYTHON_IMAGE_TAG}
ARG REQUIREMENTS=requirements-aws.txt
WORKDIR /app
COPY ${REQUIREMENTS} /app/requirements.txt
RUN python -m pip install --no-cache-dir -r /app/requirements.txt
# RDS CA bundle: the simulator's database login is verified TLS (sslmode=verify-full) with an IAM token.
RUN python -c "import urllib.request; urllib.request.urlretrieve('https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem', '/app/rds-global-bundle.pem')"
COPY eocr_sim.py console.py console.html store.py /app/
RUN useradd --system --uid 10001 eocr
USER eocr
# 8080: eOCR callback endpoint (Lattice target). 8081: operator console (ALB /eocr-sim/* with ACE-user sign-in when
# SIM_CONSOLE_HOST=0.0.0.0; loopback + SSM port-forward otherwise).
EXPOSE 8081
EXPOSE 8080
CMD ["python", "/app/eocr_sim.py", "serve"]
