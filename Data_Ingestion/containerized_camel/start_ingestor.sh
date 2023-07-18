#!/bin/bash

docker build . -t tap_project:data_ingestor && docker run -v datastorage:/datastorage --network tap_project --name dataIngestor -it tap_project:data_ingestor