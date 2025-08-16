FROM confluentinc/cp-kafka-connect-base:7.7.1

ENV CONNECT_PLUGIN_PATH="/usr/share/java,/usr/share/confluent-hub-components"

RUN echo "--> Installing Connector Plugins"

# Sink/Source - https://github.com/mongodb/mongo-kafka/blob/master/README.md
RUN confluent-hub install --no-prompt neo4j/kafka-connect-neo4j:5.1.13

# PostgreSQL Sink https://docs.confluent.io/kafka-connectors/jdbc/current/sink-connector/overview.html
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.8.0

USER appuser
