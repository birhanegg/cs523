
# Big Data Technology Project: September 2021
-----
  - Real Time tweets analysis to get top hashtags using Apache Kafka, Apache Spark, and Hive in Cloudera quickstart VM environment. 
  - A Spark Streaming application that receives tweets on time interval and push into kafka topic then in Hbase. 

# work flow 
 - twitter steam data --> spark streaming --> kafka ---> kafkaConsumer--> HBase --> Hue(dashboard)
 -  for more information refere to PowerPoint. 
# configurations 
- $> sudo service hbase-master start
- $> sudo service hbase-regionserver start
- $> service --status-all
- $> bin/kafka-server-start.sh config/server.properties
- $> bin/kafka-topics.sh --create --topic twitter-events --bootstrap-server localhost:9092
----
# installation and runing 
- import the source code into eclipse IDE & maven install and run 

## sample input and output
- already available in the PowerPoint

## github and video links 
- https://github.com/birhanegg/cs523
- https://web.microsoftstream.com/video/d15ace5f-4bf6-45c9-885a-b8701ca9c8ea

## Limitation
 - spark run in local mode 