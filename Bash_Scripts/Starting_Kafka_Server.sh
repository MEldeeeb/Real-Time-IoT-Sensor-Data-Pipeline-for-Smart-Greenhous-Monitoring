#!/bin/bash
sleep 10
# Start Kafka
echo "Starting Kafka..."
cd C:/Bigdata_kafka/kafka
./bin/windows/kafka-server-start.sh ./config/server.properties 
