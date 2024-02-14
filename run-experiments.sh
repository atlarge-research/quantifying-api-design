#!/bin/bash
cp trace_files/azure.parquet plot/trace/azure/meta.parquet
cp trace_files/google.parquet plot/trace/google/meta.parquet
cp trace_files/bitbrains.parquet plot/trace/bitbrains/meta.parquet
cp trace_files/google_ibm.parquet plot/trace/ibm/tasks.parquet

./gradlew :opendc-experiments:studying-apis:reservations:experiment --rerun-tasks
cp -r opendc-experiments/studying-apis/reservations/output/azure/* plot/input/reservations/azure/.

./gradlew :opendc-experiments:studying-apis:migrations:experiment --rerun-tasks
cp -r opendc-experiments/studying-apis/migrations/output/azure/* plot/input/migrations/azure/.
cp -r opendc-experiments/studying-apis/migrations/output/google/* plot/input/migrations/google/.
cp -r opendc-experiments/studying-apis/migrations/output/bitbrains/* plot/input/migrations/bitbrains/.

./gradlew :opendc-experiments:studying-apis:metadata:experiment --rerun-tasks
cp -r opendc-experiments/studying-apis/metadata/output/ibm/* plot/input/metadata/ibm/.

python3 plot/script/reservations_plot_results.py azure
python3 plot/script/migrations_plot_results.py azure 0.85
python3 plot/script/metadata_plot_results.py ibm
