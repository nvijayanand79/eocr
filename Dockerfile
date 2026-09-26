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
COPY eocr_sim.py /app/eocr_sim.py
RUN useradd --system --uid 10001 eocr
USER eocr
EXPOSE 8080
CMD ["python", "/app/eocr_sim.py", "serve"]
