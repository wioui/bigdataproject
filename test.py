import os

import datetime

#print(datetime.datetime(2013, 11, 5, 20, 24, 51))


import csv
import json

a=662
print(type(a))
str(a)
print(type(a))

fieldnames = ("SITE_ID", "INDUSTRY", "SUB_INDUSTRY", "SQ_FT", "LAT", "LNG", "TIME_ZONE", "TZ_OFFSET")
reader = csv.DictReader( csvfile, fieldnames)

for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write('\n')

b="2013-11-19T14:00:00Z"
c="2013-11-19T20:00:00Z"
del b[-1]
print (b)
inDate = "2012-01-01 02:20:00"
d = datetime.datetime.strptime(inDate, "%Y-%m-%d %H:%M:%S")
print(d)

a=db.find_one({"SITE_ID":"197"},{"_id":1})

print(a["_id"])

