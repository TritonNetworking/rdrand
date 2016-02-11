#!/usr/bin/python3.4
import subprocess
from glob import glob
import os

#
#  Move the files that were generated using rdrand from the local machine 
#  to a remote location
#

while True:
	subprocess.call(["mv"] + glob(os.path.join("../../holding/", "*")) + ["../../transferring"])
	subprocess.call(["rsync"] + ["-v"] + glob(os.path.join("../../transferring/", "*")) + ["kwetts@169.228.66.74:/kim2/data/"])
	subprocess.call(["rm"] + glob(os.path.join("../../transferring/", "*")))