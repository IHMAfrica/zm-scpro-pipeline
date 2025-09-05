import os

def kafka_security_props():
    """
    Returns Flink connector property dict for Kafka security.
    - PLAINTEXT => {}
    - SASL_*    => proper security.protocol, mechanism, jaas.config
    """
    protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    mech = os.getenv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
    user = os.getenv("KAFKA_SASL_USER") or os.getenv("KAFKA_SASL_USERNAME")
    pwd  = os.getenv("KAFKA_SASL_PASSWORD")

    if not protocol.startswith("SASL"):
        return {}

    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f"username='{user}' password='{pwd}';"
    )
    return {
        "properties.security.protocol": protocol,
        "properties.sasl.mechanism": mech,
        "properties.sasl.jaas.config": jaas,
    }
