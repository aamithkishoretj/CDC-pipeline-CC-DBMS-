# One-shot job image: registers the Debezium connector with Kafka Connect.
FROM debezium/connect:2.6
COPY shopdb-connector.json register-connector.sh /debezium/
ENTRYPOINT ["/bin/bash", "/debezium/register-connector.sh"]
