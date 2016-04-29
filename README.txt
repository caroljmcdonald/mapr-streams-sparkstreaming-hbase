
Commands to run :

Step 1: First compile the project on eclipse: Select project  -> Run As -> Maven Install

Step 2: use scp to copy the ms-sparkstreaming-1.0.jar to the mapr sandbox or cluster

also use scp to copy the data sensor.csv file from the data folder to the cluster
put this file in a folder called data. The producer reads from this file to send messages.

scp  ms-sparkstreaming-1.0.jar user01@ipaddress:/user/user01/.
if you are using virtualbox:
scp -P 2222 ms-sparkstreaming-1.0.jar user01@127.0.0.1:/user/user01/.

Create the topic

maprcli stream create -path /user/user01/pump -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/pump -topic sensor -partitions 3

get info on the topic
maprcli stream info -path /user/user01/pump


To run the MapR Streams Java producer and consumer:

java -cp ms-sparkstreaming-1.0.jar:`mapr classpath` solution.MyProducer

java -cp ms-sparkstreaming-1.0.jar:`mapr classpath` solution.MyConsumer

Step 3:  To run the Spark  streaming consumer:

start the streaming app
 
spark-submit --class solution.SensorStreamConsumer --master local[2] ms-sparkstreaming-1.0.jar

ctl-c to stop 

step 4: Spark streaming app writing to hbase

first make sure that you have the correct version of HBase installed, it should be 1.1.1:

cat /opt/mapr/hbase/hbaseversion 
1.1.1

Next make sure the Spark HBase compatibility version is correctly configured here: 
cat  /opt/mapr/spark/spark-1.5.2/mapr-util/compatibility.version 
hbase_versions=1.1.1

If this is not 1.1.1 fix it.


Create an hbase table to write to:
launch the hbase shell
$hbase shell

create '/user/user01/sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}

Step 4: start the streaming app writing to HBase
 
spark-submit --class solution.HBaseSensorStream --master local[2] ms-sparkstreaming-1.0.jar

ctl-c to stop

Scan HBase to see results:

$hbase shell

scan '/user/user01/sensor' , {'LIMIT' => 5}

scan '/user/user01/sensor' , {'COLUMNS'=>'alert',  'LIMIT' => 50}

Step 5:
exit, run spark (not streaming) app to read from hbase and write daily stats 

spark-submit --class solution.HBaseReadRowWriteStats --master local[2] ms-sparkstreaming-1.0.jar

$hbase shell

scan '/user/user01/sensor' , {'COLUMNS'=>'stats',  'LIMIT' => 50}


cleanup if you want to re-run:

maprcli stream topic delete -path /user/user01/pump -topic sensor 
maprcli stream topic create -path /user/user01/pump -topic sensor -partitions 3
hbase shell 
truncate '/user/user01/sensor'