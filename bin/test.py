import sys
import requests
import csv
import json
import datetime
import time
import re
response = requests.get('http://172.22.130.31:5000/.csv?table')


def getepoch(timestring, pattern):
    print timestring
    timestring = re.sub(r'((,|\.)\d+)', '', timestring)
    print timestring
    datetime_object = datetime.datetime.strptime(timestring, pattern)
    return datetime_object.strftime("%s")

def findtime(field, jbod):
    time = None
    for k, v in jbod.iteritems():
        if k == field:
            time = v
            break
        if isinstance(v, dict):
            time = findtime(field, v)
    return time

timefield = "a"
pattern = "%Y-%m-%dD%H:%M:%S"
txt = response.text.splitlines()
csvFile = csv.reader(txt, delimiter=',')
results = []
header = csvFile.next()
timeindex = None
for index, item in enumerate(header):
    if item == timefield:
        timeindex = index
for row in csvFile:
    record = {}
    record['_raw'] = row
    for index, item in enumerate(row):
        if not ('_time' in record):
            if index == timeindex:
                record['_time'] = int(getepoch(item, pattern))
            else:
                record['_time'] = int(datetime.datetime.now().strftime("%s"))
        record[header[index]] = item
    print record