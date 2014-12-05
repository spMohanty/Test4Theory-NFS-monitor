"""
Author : S.P. Mohanty (spmohanty91@gmail.com)
Date : 18th June, 2014

Version : 3

Daemon to monitor the output folder on the NFS server
for new .tgz files uploaded by the NFS clients after the job is complete

and also parses the jobdata file in the .tgz files and publishes to a broadcasting channel called t4tc_jobdata_broadcast 
which gets used to build an aggregated statistics which gets relayed to web clients using socket.io

The Daemon pushes a message for each tgz file that is created onto a rabbitmq server



Params : 

t4tc_folder :: A writable folder which holds the PID file and the LOG file for this daemon
rabbitmq_server :: Address/IP of the rabbitmq_server

Updates ::
Writes the analytics to a redis-server along with pushing the job_id to the rabbitMQ queue "t4tc_monitor"


Dependencies ::
Pika
Daemonize
pyinotify
redis
"""

import pyinotify
import time
import pika
import redis
from daemonize import Daemonize
import logging
import re
from multiprocessing import Process

import tarfile
import random
import json

from config import *


"""
 Runs as a separate thread and updates the event rate for all the accelerators
"""
def batchUpdates(redis_client,logger):
    frequency = 1 ##Compute every 1 second
    Event_Buffer_Expiry_Time = 60 *1000 # After how many mili seconds remove the event from the buffer
    while True:
        time.sleep(1)
        ## Iterate through all accelerators 
        pipe = redis_client.pipeline()
        accelList = ['CDF', 'STAR', 'UA1', 'DELPHI', 'UA5', 'ALICE', 'TOTEM', 'SLD', 'LHCB', 'ALEPH', 'LHCF', 'ATLAS', 'CMS', 'OPAL', 'D0', 'TOTAL']
        for namespace in accelList:
            base_hash = "T4TC_MONITOR/"+namespace+"/"
            ##Get all members in the sorted set EVENT_BUFFER (score=timestamp, key=numberofevents__jobid) and compute the average event rate and update
            pipe.zrange(base_hash+"EVENT_BUFFER", 1, -1, withscores=True)
            ##Remove all events in event buffer with score < currentimestamp - Event_Buffer
            pipe.zremrangebyscore(base_hash+"EVENT_BUFFER", 0, time.time()*1000 - Event_Buffer_Expiry_Time)
            ## Take the time difference to be Max - Min
            
        result = pipe.execute()
        pipe = redis_client.pipeline()
        for k in range(len(result)):
            if k%2 == 0: ##ZRange result
                acceleratorName = accelList[k/2]
                eventsBuffer = result[k]
                eventRate = 0
                totalEvents = 0
                minTime = time.time()*1000 + 1000 ##Higher than any timestamp in the buffer
                maxTime = 0 ## Lower than any timestamp in the buffer
                if len(eventsBuffer) > 1:
                    ## Compute only when there are atleast two events in the buffer
                    for k in eventsBuffer :
                        totalEvents+=int(k[0].split("_")[1])
                        timestamp = int(k[1])
                        if timestamp < minTime :    
                            minTime = timestamp
                        if timestamp > maxTime :
                            maxTime = timestamp
                    eventRate = (totalEvents/(maxTime-minTime))* 1000 * 60 ## Per minute

                base_hash = "T4TC_MONITOR/"+acceleratorName+"/"
                pipe.hset(base_hash, "event_rate", eventRate)   
        result = pipe.execute()
    

def parseJOBDATA(s):
        d = {}
        s = s.split("\n")
        for k in s:
                if k.strip()=="":
                        continue
                else:
                        p = k.split("=")
                        n = int(p[1]) if p[1].isdigit() else p[1]
                        d[p[0]]=n

        return d

def getJobData(fileName): #absolute path of the file
    
    ##print "FileName : ", fileName
    t = tarfile.open(fileName,"r")
    try:
        f = t.extractfile("./jobdata")
        data = f.read()
        return parseJOBDATA(data)           
    except:
        ##Add exception for corrupt tarfile later
        ## For now pass silently 
        #print "Unable to obtain jobdata for....", fileName
        pass
    return {}

def update_t4tc_analytics_on_redis(result, redis_client):
    ##print result
    # Random Accelerator Name now
    accelerator_name = random.choice( ['CDF', 'STAR', 'UA1', 'DELPHI', 'UA5', 'ALICE', 'TOTEM', 'SLD', 'LHCB', 'ALEPH', 'LHCF', 'ATLAS', 'CMS', 'OPAL', 'D0'] )
    events = result['events']

    ## Do the updates for total data and individual accelerator. Imagine the total analytics as a separate accelerator
    ##print [accelerator_name, "TOTAL"]
    for namespace in [accelerator_name, "TOTAL"]:
        base_hash = "T4TC_MONITOR/"+namespace+"/"
        #print base_hash, " ::: BASE HASH"
        # Buffer all commands using a pipeline to increase performance
        pipe = redis_client.pipeline()

        # Total jobs : Succeeded + Failed 
        pipe.hincrby(base_hash, "jobs_completed", 1)
        ## Update the sorted set of per user data
        pipe.zincrby(base_hash+"PER_USER/jobs_completed",result['AGENT_JABBER_ID'],1);

        # Check if the job was succcessfull completed
        if result['exitcode'] == 0:
            # Job sucessfully completed
            # increment events
            pipe.hincrby(base_hash,"events", events) # O(1)
            ## Update the sorted set of per user data
            pipe.zincrby(base_hash+"PER_USER/events",result['AGENT_JABBER_ID'],events);

            ## Add job_data to the EventRate_Buffer
            pipe.zadd(base_hash+"EVENT_BUFFER",int(time.time()*1000), str(result['jobid'])+"_"+str(events))
            #print "Adding data to event buffer....",base_hash+"EVENT_BUFFER",str(result['jobid'])+"_"+str(events),int(time.time()*1000)    
        else:
            # Job failed
            pipe.hincrby(base_hash, "jobs_failed", 1)  # O(1)
            ## Update the sorted set of per user data
            pipe.zincrby(base_hash+"PER_USER/jobs_failed",result['AGENT_JABBER_ID'],1);

        ## Contributing Users Set   
        pipe.sadd(base_hash+"users",result['AGENT_JABBER_ID']) # O(N) here N = 1   ## SCARD can be used to get the cardinality of this set in O(1)

         
    
        #pipe.hget(base_hash, "events_in_last_update")
        #pipe.hget(base_hash, "timestamp_of_last_update")

        redis_result = pipe.execute()
        """
        timestamp_of_last_update = redis_result[-1] 
        events_in_last_update = redis_result[-2]

        #print "Pipe execute1 : ",redis_result  

        # New pipe for processed data update
        pipe = redis_client.pipeline()
        if result['exitcode'] == 0 and timestamp_of_last_update :
            event_rate = ((int(events_in_last_update) * 1.0 )/ (int(time.time()*1000) - int(timestamp_of_last_update))) * 1000  ##Event Rate per second
            pipe.hset(base_hash, "event_rate", event_rate)
            pipe.hset(base_hash, "timestamp_of_last_update", int(time.time()*1000)) ## Linux Epoch time in miliseconds
            pipe.hset(base_hash, "events_in_last_update", events)
            
        else:
            ## Initial case
            ## To be neglected in case of a failed job
            if result['exitcode'] == 0:
                pipe.hset(base_hash, "timestamp_of_last_update", int(time.time()*1000)) ## Linux Epoch time in miliseconds
                pipe.hset(base_hash, "events_in_last_update", events)
                pipe.hset(base_hash, "event_rate", 0)
        
        #print "Pipe execute2 : ",pipe.execute()
        """

def main():
    global t4tc_folder
    global rabbitmq_server
    global redis_server
    global location_of_shared_mcplots_output_folder
    #Setup Logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    fh = logging.FileHandler(t4tc_folder+"/t4tc_monitor.log", "w")
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    keep_fds = [fh.stream.fileno()]

    ## Testing connection with RabbitMQ Server
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_server))
        channel = connection.channel()
        channel.queue_declare(queue='t4tc_monitor')

        logger.debug("Connected to the RabbitMQ Server successfully and checked/created the queue")
    except:
        logger.debug("Unable to connect to the RabbitMQ Server :'(")

    ## Testing connection with redis server 
    try:
        redis_client = redis.StrictRedis(host=redis_server, port=6379, db=0)
        logger.debug("Connected to the Redis Server")
    except:
        logger.debug("Unable to connect to the Redis Server :'(")

    
    ##Start the Batch Computing Jobs in a separate thread
    p = Process(target=batchUpdates , args = (redis_client, logger,))
    p.start()
    ## Batch compurting jobs started 

    wm = pyinotify.WatchManager()  # Watch Manager
    mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE  # watched events
    mask = pyinotify.ALL_EVENTS

    class EventHandler(pyinotify.ProcessEvent):
        def __init__(self, channel):
            self.channel =channel

        #def process_IN_CREATE(self, event):
        def process_IN_CLOSE_WRITE(self, event):
            ###print time.time(), "Creating:", event.pathname
            ###print event
            
            ##Only report creation of tgz files....removes a lot of noise
            if(not re.match(".*\.tgz$", event.pathname)):
                return  
            
        try: 
            # process and push data into redis analytics server
            update_t4tc_analytics_on_redis(getJobData(event.pathname), redis_client)
            logger.debug("Updated analytics on redis for "+event.pathname)
        except:
            logger.debug("Unable to update analytics on redis for "+event.pathname+" ")

            try :
                #publish to workers for file creation notification
                self.channel.basic_publish(exchange='',
                      routing_key='t4tc_monitor',
                      body='FILE_CREATED : '+event.pathname)
                logger.debug("FILE_CREATED : "+event.pathname)
            except:
                logger.debug("Unable to publish to RabbitMQ Server on File Create")

        def process_IN_DELETE(self, event):
            ###print time.time(), "Removing:", event.pathname
            ###print event
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
            ###print "Random Event", event
            foo=1
            # Decide if we want to log all the events or not


    handler = EventHandler(channel)
    notifier = pyinotify.Notifier(wm, handler)
    wdd = wm.add_watch(location_of_shared_mcplots_output_folder, mask, rec=True, auto_add=True) #rec=True says recursively set watchers on the subdirectories also !! We dont want to miss out on the information about the files created inside the diretories, that will help us know when the output folder is ready to be pushed for a job
    notifier.loop()
    connection.close()
    ## TO-DO::
    ## Understand the redis_client connection pooling thingy !! and figure out if we really dont need to close the connection :-?



#main()

daemon = Daemonize(app="T4TC monitor", pid=t4tc_folder+"/t4tc_monitor.pid", action = main)
daemon.start()

