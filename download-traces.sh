#!/bin/bash

curl -L https://zenodo.org/api/records/7996316/files-archive -o trace_files.zip
mkdir -p trace_files
unzip trace_files.zip -d trace_files

declare -a traces=("azure" "bitbrains" "google")
declare -a experiments=("reservations" "migrations")

for exp in "${experiments[@]}"
do
    for tr in "${traces[@]}"
    do
        mkdir -p "opendc-experiments/studying-apis/$exp/src/main/resources/trace/$tr/"
        cp "trace_files/$tr.parquet" "opendc-experiments/studying-apis/$exp/src/main/resources/trace/$tr/meta.parquet"
    done
done

mkdir -p "opendc-experiments/studying-apis/metadata/src/main/resources/trace/ibm/"
cp "trace_files/google_ibm.parquet" "opendc-experiments/studying-apis/metadata/src/main/resources/trace/ibm/tasks.parquet"
