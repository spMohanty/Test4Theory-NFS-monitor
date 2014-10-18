"""
Author : S.P. Mohanty (spmohanty91@gmail.com)
Date : 18th June, 2014

Daemon to monitor the output folder on the NFS server
for new .tgz files uploaded by the NFS clients after the job is complete

The Daemon pushes a message for each tgz file that is created onto a rabbitmq server

Params : 

t4tc_folder :: A writable folder which holds the PID file and the LOG file for this daemon
rabbitmq_server :: Address/IP of the rabbitmq_server

Dependencies ::
Pika
Daemonize
pyinotify
"""

import pyinotify
import time
import pika
from daemonize import Daemonize
import logging
import re
from config import *

print t4tc_folder

def main():
    global t4tc_folder
    global rabbitmq_server
    global location_of_shared_mcplots_output_folder
    #Setup Logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fh = logging.FileHandler(t4tc_folder+"/t4tc_monitor.log", "w")
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    keep_fds = [fh.stream.fileno()]

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_server))
        channel = connection.channel()
        channel.queue_declare(queue='t4tc_monitor')
        logger.debug("Connected to the RabbitMQ Server successfully and checked/created the queue")
    except:
        logger.debug("Unable to connect to the RabbitMQ Server :'(")
        

    wm = pyinotify.WatchManager()  # Watch Manager
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE  # watched events
    mask = pyinotify.ALL_EVENTS

    class EventHandler(pyinotify.ProcessEvent):
        def __init__(self, channel):
            self.channel =channel

        def process_IN_CREATE(self, event):
            #print time.time(), "Creating:", event.pathname
            #print event
            
            ##Only report creation of tgz files....removes a lot of noise
            if(not re.match(".*\.tgz$", event.pathname)):
                return  

            try :
                self.channel.basic_publish(exchange='',
                      routing_key='t4tc_monitor',
                      body='FILE_CREATED : '+event.pathname)
                logger.debug("FILE_CREATED : "+event.pathname)
            except:
                logger.debug("Unable to publish to RabbitMQ Server on File Create")

        def process_IN_DELETE(self, event):
            #print time.time(), "Removing:", event.pathname
            #print event
            ##Only report deletion of tgz files....removes a lot of noise
            if(not re.match(".*\.tgz$", event.pathname)):
                return  
            
            # Commenting out publishing of FILE DELETE notification to the rabbitmq queue
            #try :
            #    self.channel.basic_publish(exchange='',
            #          routing_key='t4tc_monitor',
            #          body='FILE_DELETED : '+event.pathname)
            #    logger.debug("FILE_DELETED : "+event.pathname)
            #except:
            #    logger.debug("Unable to publish to RabbitMQ Server on File Delete")
            logger.debug("FILE_DELETED : "+event.pathname)
            
        def process_default(self,event):
            #print "Random Event", event
            foo=1
            # Decide if we want to log all the events or not


    handler = EventHandler(channel)
    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch(location_of_shared_mcplots_output_folder, mask, rec=True, auto_add=True) #rec=True says recursively set watchers on the subdirectories also !! We dont want to miss out on the information about the files created inside the diretories, that will help us know when the output folder is ready to be pushed for a job
    notifier.loop()
    connection.close()



daemon = Daemonize(app="T4TC monitor", pid=t4tc_folder+"/t4tc_monitor.pid", action = main)
daemon.start()

