#!/usr/bin/python3.4
import time
import subprocess
import os

#
#  Continuously generate 10 GB files using rdrand
#

startTime = time.time()

i = 0
while i < 12800:
	# Make filenames all the same length so they're in the correct order
	if i < 10:
		filename = "0000" + str(i) + ".txt"
	elif i < 100:
		filename = "000" + str(i) + ".txt"
	elif i < 1000:
		filename = "00" + str(i) + ".txt"
	else:
		filename = "0" + str(i) + ".txt"

	# Get the amount of files currently in the 'holding' directory
	ls = subprocess.Popen(("ls", "-1", "../../holding"), stdout=subprocess.PIPE)
	amt_hold = subprocess.check_output(("wc", "-l"), stdin=ls.stdout)
	ls.wait()

	# Get the amount of files currently in the 'transferring' directory
	ls = subprocess.Popen(("ls", "-1", "../../transferring"), stdout=subprocess.PIPE)
	amt_transfer = subprocess.check_output(("wc", "-l"), stdin=ls.stdout)
	ls.wait()

	# Convert byte arrays to ints
	amt_hold = int(amt_hold)
	amt_transfer = int(amt_transfer)

	# Make sure that the generating machine doesn't fill up disk space
	if (amt_hold + amt_transfer) < 60:
		subprocess.call(["../rdrand", "-s", "10g", "-o", "../../generating/" + filename, "-c", "0"])
		os.sync()
		subprocess.call(["mv", "../../generating/" + filename, "../../holding"])
		print("Finished generating " + filename)
		i += 1

endTime = time.time()
timeElapsed = endTime - startTime

print("Time elapsed: " + str(timeElapsed) + " seconds")