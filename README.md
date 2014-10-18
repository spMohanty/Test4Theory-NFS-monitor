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

#Author
=======
S.P. Mohanty   
spmohanty91@gmail.com   