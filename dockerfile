# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.10-slim

WORKDIR /app
ENV DAGSTER_HOME=/app

COPY . /app

# Set $DAGSTER_HOME and copy dagster instance
RUN pip install -r /app/requirements.txt
