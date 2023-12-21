FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update
# RUN apt-get install -y gcc python3-dev
RUN apt-get install -y openjdk-11-jdk
RUN apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow
RUN pip install pyspark==3.2.1
RUN pip install 
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
# RUNsudo chmod 777  /usr/local/bin/*

