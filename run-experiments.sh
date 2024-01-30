#!/bin/bash

./gradlew :opendc-experiments:studying-apis:migrations:experiment --rerun-tasks
cp trace_files/azure.parquet plot/trace/azure/meta.parquet
cp -r opendc-experiments/studying-apis/migrations/output/azure/* input/migrations/azure/.

./gradlew :opendc-experiments:studying-apis:metadata:experiment --rerun-tasks
cp trace_files/google_ibm.parquet plot/trace/ibm/tasks.parquet
cp -r opendc-experiments/studying-apis/metadata/output/ibm/* plot/input/metadata/ibm/.

python3 plot/script/migrations_plot_results.py azure 0.85
python3 plot/script/metadata_plot_results.py ibm
