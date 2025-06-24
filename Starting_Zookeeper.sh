#!/bin/bash

# Start Zookeeper
echo "Starting Zookeeper..."
cd /c/Bigdata_kafka/kafka
./bin/windows/zookeeper-server-start.sh ./config/zookeeper.properties
