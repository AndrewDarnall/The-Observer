#!/bin/bash

wget -P $(pwd)/Data_Streaming/setup/ https://dlcdn.apache.org/kafka/3.4.1/kafka_2.13-3.4.1.tgz &&
wget -P $(pwd)/Data_Processing/setup/ https://dlcdn.apache.org/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz &&
docker network create --subnet=10.0.100.1/24 tap_project_2 &&
docker volume create datastorage 
