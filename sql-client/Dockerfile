# Start with the Confluent Kafka Connect base image
FROM confluentinc/cp-kafka-connect-base:5.3.2

# Set the Kafka Connect plugin path
ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

# Install MongoDB Kafka Connector
RUN confluent-hub install --no-prompt mongodb/kafka-connect-mongodb:latest

# Switch to a new stage for Flink

FROM flink:1.16-java11

# Create CLI lib folder
COPY sql-client/bin/* /opt/sql-client/
RUN mkdir -p /opt/sql-client/lib

# Download connector libraries
RUN wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.0/flink-sql-connector-kafka-1.16.0.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.16.0/flink-json-1.16.0.jar; \
    wget -P /opt/sql-client/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.16.0/flink-sql-avro-confluent-registry-1.16.0.jar;

# Copy configuration
COPY sql-client/conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client

COPY sql-client/docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]

