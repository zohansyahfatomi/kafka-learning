from confluent_kafka import Consumer, KafkaException
import sys
import getopt
import json
import logging
from pprint import pformat
import pymysql

#db connec
def openDb():
   global conn, cursor
   conn = pymysql.connect(
       host="localhost",
       user="root",
       password="",
       db="db_kafka" )
   cursor = conn.cursor()	

#db closing conn
def closeDb():
   global conn, cursor
   cursor.close()
   conn.close()

if __name__ == '__main__':
    broker = 'localhost:9092'
    group = 'test group'
    conf = {'bootstrap.servers': broker, 'group.id': group, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}

    c = Consumer(conf)

    
    
    # Subscribe to topics
    topics = ['bukan_testing']
    c.subscribe(topics)
    
    #inserting msg into database
    #msg = c.poll(timeout=1.0) #getting from subscribe
    
 


    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                #inserting msg into database
                print(msg.value())
                openDb()
                sql = "INSERT INTO Msg (Kata) VALUES (%s);"
                val = msg.value()
                cursor.execute(sql, val)
                conn.commit()   
                closeDb()
                
    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    finally:
        # Close down consumer to commit final offsets.
        c.close()

