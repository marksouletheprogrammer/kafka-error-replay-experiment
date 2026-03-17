FROM confluentinc/cp-kafka-connect:7.7.1

USER root

# Install JDBC Sink Connector and PostgreSQL driver
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0

# Copy PostgreSQL JDBC driver
RUN curl -L -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/postgresql-42.7.4.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

USER appuser
