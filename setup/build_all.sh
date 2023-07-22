#!/bin/bash

# Simple build script for ease of deployment

docker build ./stream_connector/ -t tap_project:stream_connector_2 && docker build ./data_ingestor/ -t tap_project:data_ingestor && docker build ./data_streamer --tag tap_project:kafka