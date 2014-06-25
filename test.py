import time
from random import randint
import cassandra
from cassandra.cluster import Cluster
import sys

if len(sys.argv) < 2:
    print "You must provide the metric name"
    sys.exit(1)

metricName = sys.argv[1]

cluster = Cluster()
session = cluster.connect()

try:
    session.execute("""
        CREATE KEYSPACE timeseries WITH replication
            = {'class':'SimpleStrategy', 'replication_factor':3};
        """)
except cassandra.AlreadyExists:
    print 'timeseries key space already exists'

try:
    session.execute("""CREATE TABLE timeseries.data_points (
         metric_name text,
         value int,
         time timestamp,
         PRIMARY KEY ((metric_name), time)
        ) WITH COMPACT STORAGE;""")
except cassandra.AlreadyExists:
    print 'data_points table already exists'

for i in range(10):
    session.execute("INSERT INTO timeseries.data_points (metric_name, time, value)\
        VALUES('%s', %d, %d);" % (metricName, int(round(time.time() * 1000)), randint(0, 10)))

results = session.execute("""SELECT * FROM timeseries.data_points""")

print "%-15s\t%-25s\t%-20s\n%s" % \
      ("Name", "Timestamp", "Value",
       "---------------+-------------------------------+------------------------")

for row in results:
    print "%-15s\t%-25s\t%-20s" % (row.metric_name, row.time, row.value)