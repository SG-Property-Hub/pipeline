FROM apache/airflow:2.7.0

USER root
# RUN a2enmod rewrite
# RUN echo "deb https://deb.debian.org/debian/ stable main" > /etc/apt/sources.list
# RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt clean
RUN apt-get update
RUN apt-get install openjdk-11-jdk -y
RUN rm -rf /var/lib/apt/lists/*   

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin
RUN echo $JAVA_HOME

USER airflow

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark