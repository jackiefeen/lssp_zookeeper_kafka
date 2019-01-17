Large Scale Systems Project - ReadMe

Authors:
Gioele Bigini (https://github.com/BigG-DSC): gioele.bigini@gmail.com
Jacqueline Neef (https://github.com/jackiefeen): jacqueline.neef93@gmail.com

EIT Digital Data Science
Universidad Politécnica de Madrid
17-01-2019

The goal of this project is to build a distributed chat application using Java 8, Apache ZooKeeper and Apache Kafka.

Running our application:

Use a Linux, e.g. Ubuntu, environment (x86_64) with the following software installed:
- Java 8
- maven
- Apache ZooKeeper version 3.4.13
- Apache Kafka version 2.11-1.0.0


Preliminary steps:
- clean the kafka logs (default location: /tmp/kafka-logs)
- reset the ZooKeeper tree structure to default
- in the Kafka config/server.properties file set:
num.partitions=2
auto.create.topics.enable = false


Build and run the application:
In the lssp_zookeeper_kafka folder run:
- mvn clean install
- run the jar of the manager in a terminal
- run the jar of one or multiple clients in a terminal

By default, the chat application connects to localhost:2181 (default ZooKeeper hostport), but can also be used in a distributed manner with IP:port.







