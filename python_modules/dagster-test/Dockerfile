ARG PYTHON_VERSION

FROM "python:${PYTHON_VERSION}-slim"

COPY libraries /
COPY dagster /libraries
COPY dagit /libraries

RUN pip install \
    -e libraries/dagster \
    -e libraries/dagster-graphql \
    -e libraries/dagster-celery[flower,redis,kubernetes] \
    -e libraries/dagster-cron \
    -e libraries/dagit \
    -e libraries/dagster-postgres \
    -e libraries/dagster-pandas \
    -e libraries/dagster-aws \
    -e libraries/dagster-gcp \
    -e libraries/dagster-k8s \
    -e libraries/dagster-celery-k8s \
    -e libraries/dagster-celery-docker \
    -e libraries/dagster-docker \
    -e libraries/dagster-airflow
#
# ENV GOOGLE_APPLICATION_CREDENTIALS="/modules/gac.json"

ENV DAGSTER_DISABLE_TELEMETRY=true

# This makes sure that logs show up immediately instead of being buffered
ENV PYTHONUNBUFFERED=1

WORKDIR /dagster_test/test_project/

EXPOSE 80
