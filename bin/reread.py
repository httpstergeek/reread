
__author__ = ''
import json
import splunk.clilib.cli_common as spcli
import splunk.Intersplunk
import sys
import requests
import datetime
import csv
import re

row={}
results=[]
keywords, options = splunk.Intersplunk.getKeywordsAndOptions()

def getepoch(timestring, pattern):
    timestring = re.sub(r'((,|\.)\d+)', '', timestring)
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

def main(args):
    option = args[1]
    url = args[2]
    timefield = args[3]
    pattern = args[4]
    response = requests.get(url)
    txt = response.text.splitlines()
    if option == 'csv':
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
                if index == timeindex:
                    if not ('_time' in record):
                        record['_time'] = int(getepoch(item, pattern))
                record[header[index]] = item
            results.append(record)
        splunk.Intersplunk.outputStreamResults(results)
        exit()

    if option == 'json':
        results = []
        for line in txt:
            record = json.loads(line)
            time = findtime(timefield, record)
            if time:
                record['_time'] = int(getepoch(time, pattern))
            else:
                record['_time'] = int(datetime.datetime.now().strftime("%s"))
            record['_raw'] = line
            results.append(record)
        splunk.Intersplunk.outputStreamResults(results)
        exit()
main(sys.argv)
