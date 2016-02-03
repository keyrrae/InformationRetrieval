import os

for i in xrange(1,16):
	os.system("cp apache1.splunk.com.log apache" + str(i*3+1) +".splunk.com.log")
	os.system("cp apache2.splunk.com.log apache" + str(i*3+2) +".splunk.com.log")
	os.system("cp apache3.splunk.com.log apache" + str(i*3+3) +".splunk.com.log")
