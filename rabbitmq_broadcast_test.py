#!/usr/bin/env python


import pika
from config import *

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
channel = connection.channel()

channel.queue_declare(queue='t4tc_jobdata_broadcast')

print ' [*] Waiting for messages. To exit press CTRL+C'

def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)

channel.basic_consume(callback,
                      queue='t4tc_jobdata_broadcast',
                      no_ack=True)

channel.start_consuming()
