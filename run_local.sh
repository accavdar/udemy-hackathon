#!/usr/bin/env bash

if [ "$#" -lt 2 ]; then
    echo "Usage:\nsh run.sh class_name input_path\n"
    echo "Example:\nsh run_local.sh com.udemy.hackathon.recommendation.MovieLensALS /Users/accavdar/Development/data/movieLens"
    exit 1
fi

CLASS_NAME=$1
INPUT_PATH=$2

mvn clean install shade:shade
spark-submit --driver-memory 4G --class $CLASS_NAME --master local[*] target/udemy-hackathon-uber-1.0.0.jar $INPUT_PATH
