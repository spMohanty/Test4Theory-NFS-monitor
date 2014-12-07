#!/bin/bash

kill -9 `ps uax | grep "python monitor.py" | awk '{print $2}'`
python monitor.py
