class Defaults(object):
    """Default parameters for a typical installation"""

    # host and port of the message manager (Kafka broker)
    KAFKA_HOST = "localhost"
    KAFKA_PORT = 9092
    KAFKA_LOOP_DELAY = 1000  # millisecond
    KAFKA_AUTO_OFFSET_RESET = "latest"
