# ---- Stage 1: Build custom SMT ----
FROM maven:3.9-eclipse-temurin-17 AS smt-build
COPY --from=custom-smt . /build
WORKDIR /build
RUN mvn -q -B package -DskipTests

# ---- Stage 2: Kafka Connect image ----
FROM confluentinc/cp-kafka-connect:7.7.1

USER root

# Install Confluent JDBC Sink Connector
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0

# Install Debezium JDBC Sink Connector (used by the replay connector)
RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/2.7.3.Final/debezium-connector-jdbc-2.7.3.Final-plugin.tar.gz | \
    tar -xzf - -C /usr/share/confluent-hub-components/

# Copy PostgreSQL JDBC driver
RUN curl -L -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Copy custom SMT JAR
RUN mkdir -p /usr/share/confluent-hub-components/custom-smt
COPY --from=smt-build /build/target/custom-smt-*.jar /usr/share/confluent-hub-components/custom-smt/

USER appuser
