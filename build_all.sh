#!/bin/bash

# Simple build script for ease of deployment

docker build ./Stream_Connector/ -t tap_project:stream_connector_2 && docker build ./Data_Ingestion/containerized_camel/ -t tap_project:data_ingestor && docker build ./Data_Streaming --tag tap_project:kafka