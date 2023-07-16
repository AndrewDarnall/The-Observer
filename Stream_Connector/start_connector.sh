#!/bin/bash

docker build . -t tap_project:stream_connector_2 && docker run -v datastorage:/app/datastorage --network tap_project --name streamConnector -it tap_project:stream_connector_2