import datetime

with open('readme', 'a') as log:
 now = datetime.datetime.now()
 log.write("Checked system clock status at " + now.strftime("%Y-%m-%d %H:%M:%S") + "\n")
