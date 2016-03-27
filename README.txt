
Commands to run labs:

Step 1: First compile the project on eclipse: Select project  -> Run As -> Maven Install

Step 2: use scp to copy the ms-sparkstreaming-1.0.jar to the mapr sandbox or cluster

also use scp to copy the data sensor.csv file from the data folder to the cluster
put this file in a folder called data

scp  ms-sparkstreaming-1.0.jar user01@ipaddress:/user/user01/.
if you are using virtualbox:
scp -P 2222 ms-sparkstreaming-1.0.jar user01@127.0.0.1:/user/user01/.

Create the topic

maprcli stream create -path /user/user01/pump -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /user/user01/pump -topic sensor -partitions 3



To run the MapR Streams producer and consumer:

java -cp ms-sparkstreaming-1.0.jar:`mapr classpath` solution.MyProducer

java -cp ms-sparkstreaming-1.0.jar:`mapr classpath` solution.MyConsumer

To run the  streaming consumer:

Step 3: start the streaming app
 
spark-submit --class solution.SensorStreamConsumer --master local[2] ms-sparkstreaming-1.0.jar

step 4: writing to hbase


Create an hbase table to write to:
launch the hbase shell
$hbase shell

create '/user/user01/sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}

Step 3: start the streaming app writing to HBase
 
spark-submit --class solution.HBaseSensorStream --master local[2] ms-sparkstreaming-1.0.jar

Scan HBase to see results:


$hbase shell

scan '/user/user01/sensor' , {'LIMIT' => 5}






