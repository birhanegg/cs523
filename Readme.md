
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
 # installation and run ing 
-------
import the source code into eclipse IDE
## sample input and output
- already available in the PowerPoint
## Limitation
 - spark run in local mode 