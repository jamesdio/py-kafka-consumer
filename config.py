class Config(object):
    DEBUG = True
    DEVELOPMENT = True

    # InfluxDB
    INFLUX_DB_SERVER = '127.0.0.1'
    INFLUX_DB_ID = 'admin'
    INFLUX_DB_PW = 'admin'
    INFLUX_DB_PORT = '8086'
    INFLUX_DB_NAME = 'oracle'
    INFLUX_WRITE_BATCH_SIZE = 10000

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
    KAFKA_TOPICS = ['oracle-metrics']
    KAFKA_CLIENT_ID = 'localhost'
    KAFKA_GROUP_ID = 'localhost1-1'
    KAFKA_AUTO_OFFSET_RESET = 'latest'
    KAFKA_ENABLE_AUTO_COMMIT = True
    KAFKA_CONSUMER_TIMEOUT_MS = 5000
    KAFKA_MAX_POLL_RECORDS = 1


class ProductionConfig(Config):
    DEVELOPMENT = False
    DEBUG = False

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    DEBUG = True