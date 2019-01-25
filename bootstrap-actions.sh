#!/bin/bash

#Script is used to copy the corresponding JAR-File (Flink or Spark job) to the EMR cluster

set -e
cd ~
mkdir jarfile
cd jarfile
pwd
aws s3 cp $1 ./
ls
