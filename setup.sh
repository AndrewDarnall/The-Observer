#!/bin/bash

# setup script to prepare the environment, for ease of execution
docker volume create datastorage && docker network create --subnet=10.0.100.1/24 tap_project_2