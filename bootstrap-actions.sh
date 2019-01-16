#!/bin/bash
set -e
cd ~
mkdir jarfile
cd jarfile
pwd
aws s3 cp $1 ./
ls
