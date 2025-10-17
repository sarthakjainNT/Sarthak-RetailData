FROM apache/airflow:2.11.0

USER root

# Set Java env
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Java
RUN apt-get update && apt-get install -y openjdk-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY jars/mysql-connector-j-9.4.0.jar /opt/airflow/jars/mysql-connector-j-9.4.0.jar

USER airflow

# Python
RUN pip install --no-cache-dir pyspark==3.5.6