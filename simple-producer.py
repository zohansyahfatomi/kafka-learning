from confluent_kafka import Producer
import sys, pymysql

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


def delivery_callback(err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                            (msg.topic(), msg.partition(), msg.offset()))

if __name__ == '__main__':
    broker = "localhost:9092"
    conf = {'bootstrap.servers': broker}

    # Create Producer instance
    p = Producer(**conf)

    topic = "bukan_testing"

    # Read lines from stdin, produce each line to Kafka
    for line in sys.stdin:
        try:
            # Produce line (without newline)
            p.produce(topic, line.rstrip(), callback=delivery_callback)

        except BufferError:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(p))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        p.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries\n' % len(p))
    p.flush()

#ini testing pakai gpg
    