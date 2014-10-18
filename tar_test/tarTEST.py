#!/usr/bin/env python
import tarfile

t = tarfile.open("myTAR.tgz")
f = t.extractfile("./jobdata")
print f.read().split("\n")
