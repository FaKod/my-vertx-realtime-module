# What?
This example here should show how to use components like [Kafka](http://kafka.apache.org/) and [Vertx](http://vertx.io/) for realtime analysis of data streams.

# Additional Information
* [Realtime Analysis with Kafka and Vertx](http://blog.fakod.eu/2014/10/17/realtime-analysis-kafka-vertx/)

# Example Setup (idea)
* Various components are sending [AVRO](http://avro.apache.org/) serialized log- or data-messages to Kafka
* [CAMUS](https://github.com/linkedin/camus) as a Kafka consumer stores the messages to [HDFS](http://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_distributed_file_system) for further MapReduce processing
* This Vertx project as a Kafka consumer processes the messages as well, acting as a Stream Processing component
* The processed data is shown on a [D3.js](http://d3js.org/) based Dashboard

# Stream Processing
Process of extracting knowledge structures from continuous, rapid and unlimited data.

I am using in this project:

* [StreamLib](https://github.com/addthis/stream-lib): Great set of implemented stream processing algorithms
* Top-k: Efficient Computation of Frequent and Top-k Elements in Data Streams ([paper](https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf))
* Adoptive Counting: Fast and accurate counting of unique elements ([paper](http://gridsec.usc.edu/files/TR/TR-2005-12.pdf))
* Sliding Window: Only considering data in a certain time window

# Verticles
* **MainVerticle:** Starts the following Verticles
* **KafkaVerticle:** Kafka consumer Verticle, consumes the configured set of Topics
* **WebSocketVerticle:** Manages frontend WebSocket connections
* **DataVerticle:** Processes received messages and calculates messages for Frontend

# Start this
vertx uninstall com.holidaycheck~my-vertx-realtime-module~1.0-SNAPSHOT

vertx runmod com.holidaycheck~my-vertx-realtime-module~1.0-SNAPSHOT -conf config.json