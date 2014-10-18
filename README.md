#Test 4 Theory - NFS Monitoring daemon
======================================
This daemon monitors a particular directory on the NFS server for the job data files collected in the copilot-output folder.   
This, then analyzes the jobdata file, and updates the statistics on a redis server, and simulatenously pushes the jobdata file into a rabbitmq job queue where a set of workers can pick it up from and process as needed.

#Installation
============
```bash
git clone https://github.com/spMohanty/Test4Theory-NFS-monitor
cd Test4Theory-NFS-monitor
pip install -r requirements.txt
```

#Usage
======
* Copy the ```config.py.default``` to ```config.py```
* Fill all the values of the parameters
* then run
```bash
python monitor.py
```

#Where to run ? NFS server or NFS client ?
===========================================
This daemon is ideally supposed to be run on the NFS server itself, as the ```inotify``` events, this daemon depends on are not supported by the RDP protocol of NFS. So, if for some reason, you cannot run this daemon on the NFS server, then you can also run this daemon on the client by :   
* Optimize your NFS client by following : http://nfs.sourceforge.net/nfs-howto/ar01s05.html
* add a cron job to run
```bash
cd $PATH_TO_DIRECTORY_BEING_MONITORED;find -type f -exec touch {} \; 
```
   
And how frequently you run the cron job depends on how much data is there in the folder being monitored, and how much latency can you live with.   
What happens here, is when you try to touch all the known files, the NFS client is forced to try and update your copy of the shared folder by querying the NFS server. And in case of new files, the file gets created on the client machine, and hence the ```inotify``` events are released by the client's kernel.   
This is totally not the way to go, if the timestamp of the creation/deletion of the file is critical to your case.   
(If any1 knows of a clever hack to get the ```inotify``` events on the client for a NFS shared directory, pleaseee shoot me a mail !!)   

#Author
=======
S.P. Mohanty   
spmohanty91@gmail.com   