#!/bin/bash

# Removes HADOOP_* environment variables inherited by the tdmq-client image.
for varname in ${!HADOOP*}; do unset ${varname}; done

# Runs the dpc script
python3 dpc.py $@
