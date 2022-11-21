# -*- coding: utf-8 -*-
import config
from time import time
from flask import Flask, request, current_app as app
from datetime import datetime
from influxdb import InfluxDBClient 
from influxdb.exceptions import InfluxDBClientError
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from json import loads

# for logging
logging.basicConfig()
log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.WARNING)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

class InfluxDBWriter():
    def __init__(self, host, port, user, passwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname

    def writeData(self, data):
        try:
            write_client = InfluxDBClient(host= self.host, port= self.port, username= self.user, password= self.passwd, database= self.dbname)
            # Write data to Target DB
            write_client.write_points(data, database='oracle', time_precision='s', batch_size=app.config['INFLUX_WRITE_BATCH_SIZE'], protocol='line')
            
        except InfluxDBClientError as err:
            row_count = 0
            log.error('Write to database failed: %s' % err)

class KafkaSender():
    def __init__(self, bootstarap_servers, topics, client_id, group_id, auto_offset_reset, enable_auto_commit, consumer_timeout_ms, max_poll_records):
        self.bootstrap_servers = bootstarap_servers
        self.topics = topics
        self.client_id = client_id
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.consumer_timeout_ms = consumer_timeout_ms
        self.max_poll_records = max_poll_records
        
    def consumeMessage(self, data):
        try:
            
            consumer = KafkaConsumer(bootstrap_servers = self.bootstrap_servers, client_id = self.client_id, group_id = self.group_id, auto_offset_reset = self.auto_offset_reset,
                                     enable_auto_commit = self.enable_auto_commit, consumer_timeout_ms = self.consumer_timeout_ms, max_poll_records = self.max_poll_records, value_deserializer=lambda v: loads(v.decode('utf-8')))
            '''
            consumer = KafkaConsumer(bootstrap_servers = self.bootstrap_servers, client_id = self.client_id, group_id = self.group_id, auto_offset_reset = self.auto_offset_reset,
                                     enable_auto_commit = self.enable_auto_commit, consumer_timeout_ms = self.consumer_timeout_ms, max_poll_records = self.max_poll_records)
            '''
            ### KafkaConsumer(value_deserializer=lambda v: json.loads(v.decode('utf-8')))
            consumer.subscribe(self.topics)

            print("################# DEBUG ################# ", consumer)

            tmpArray = []

            for message in consumer: 
                #print("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s" % ( message.topic, message.partition, message.offset, message.key, message.value[1] ))
                #print("################# DEBUG ################# ", message.topic, message.value, type(message.value))

                #print(type(message.value.decode('utf-8')))
                #print(message.value.decode('utf-8'))

                resultDict = message.value
                resultArray = resultDict['metrics']
                #print(type(resultArray))
                #tmpArray = tmpArray + resultArray

                ### write data to influx
                ## Write Datapoints to target Influx DB
                influx_db_host = app.config['INFLUX_DB_SERVER']
                influx_db_port = app.config['INFLUX_DB_PORT']
                influx_db_user = app.config['INFLUX_DB_ID']
                influx_db_password = app.config['INFLUX_DB_PW']
                influx_dbname = app.config['INFLUX_DB_NAME']

                influxWriter = InfluxDBWriter(influx_db_host, influx_db_port, influx_db_user, influx_db_password, influx_dbname)
                influxWriter.writeData(resultArray)

            #print(tmpArray())

        except KafkaError as err:
            row_count = 0
            log.error('consume message failed: %s' % err)
        finally:
            data = tmpArray


def kafka_consumer():

    data = []

    ## Kafka - Consume Messages
    kafkaBootStrapServers = app.config['KAFKA_BOOTSTRAP_SERVERS']
    kafkaTopics = app.config['KAFKA_TOPICS']
    kafkaClientId = app.config['KAFKA_CLIENT_ID']
    kafkaGroupId = app.config['KAFKA_GROUP_ID']
    kafkaAutoOffsetReset = app.config['KAFKA_AUTO_OFFSET_RESET']
    kafkaEnableAutoCommit = app.config['KAFKA_ENABLE_AUTO_COMMIT']
    kafkaConsumerTimeoutMs = int(app.config['KAFKA_CONSUMER_TIMEOUT_MS'])
    kafkaMaxPollRecords = int(app.config['KAFKA_MAX_POLL_RECORDS'])

    kafkaSender = KafkaSender(kafkaBootStrapServers, kafkaTopics, kafkaClientId, kafkaGroupId, kafkaAutoOffsetReset, kafkaEnableAutoCommit, kafkaConsumerTimeoutMs, kafkaMaxPollRecords)
    kafkaSender.consumeMessage(data)
    
    
    #print(data)

    '''

    '''

sched = BackgroundScheduler(daemon=True)
# debugging
# sched.add_job(get_app_id,'cron',hour='13',minute='31')
# sched.add_job(get_app_id,'interval', seconds=30)
# sched.add_job(write_smd_bypass_status, 'interval', seconds=30)
sched.add_job(kafka_consumer, 'interval', seconds=30)
sched.start()

app = Flask(__name__)

app.config['JSON_ADD_STATUS'] = False
app.config['JSON_AS_ASCII'] = False

if __name__ == '__main__':
    log = logging.getLogger()
    #log.setLevel(logging.DEBUG) #DEBUG
    log.setLevel(logging.INFO) #DEBUG
    #log.setLevel(logging.WARNING) #for Production
    app.debug = False
    app.config.from_object('config.ProductionConfig')
    app.run(host='0.0.0.0', port=5070)