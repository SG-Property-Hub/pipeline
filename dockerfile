# Dagster libraries to run both dagster-webserver and the dagster-daemon. Does not
# need to have access to any pipeline code.

FROM python:3.10-slim


# Set $DAGSTER_HOME
WORKDIR /opt/dagster/dagster_home
ENV DAGSTER_HOME=/opt/dagster/dagster_home

#copy instance
COPY dagster.yaml workspace.yaml  $DAGSTER_HOME

COPY requirements.txt $DAGSTER_HOME

RUN pip install -r /opt/dagster/dagster_home/requirements.txt
