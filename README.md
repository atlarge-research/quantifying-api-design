<a href="https://opendc.org/">
    <img src="https://opendc.org/img/logo.png" alt="OpenDC logo" title="OpenDC" align="right" height="100" />
</a>

# Evaluation of Datacenter Scheduler Programming Abstractions

This repository contains the evaluation code and experiments written in Kotlin for the research work that investigates the performance impact of various datacenter scheduler programming abstractions.

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](/LICENSE.txt)
[![Documentation](https://img.shields.io/badge/docs-master-green.svg)](./docs)
[![GitHub release](https://img.shields.io/github/release/atlarge-research/opendc)](https://github.com/atlarge-research/opendc/releases)
[![Build](https://github.com/atlarge-research/opendc/actions/workflows/build.yml/badge.svg)](https://github.com/atlarge-research/opendc/actions/workflows/build.yml)

### Prerequisites
- An x86 machine with at least 16GB RAM.
- Any desktop Linux distribution with build tools installed. For example, `build-essential` package on Ubuntu.
- Java 17 or greater.
- Python 3.11 (We recommend conda, virtualenv, or a similar environment).

### Reproducibility instructions
- Run `./download-traces.sh`. This will download all required traces from Zenodo and copy them to the relevant directories for experiments.
- Setup a python 3.11 virtual environment. Install dependencies using `pip install -r plot/script/requirements.txt`.
- Use `./run-experiments.sh` to run the simulations and plot the results.
- Figure 7 is `plot/output/migrations-results-packing-azure.pdf`. Figure 8 is `plot/output/migrations-results-totaltime-azure.pdf`. Figure 10 is `plot/output/metadata-results-ibm.pdf`.

## Getting Started

Detailed instructions to build individual experiments and modify them start here. The code for all the specific experiments can be found in the `opendc-experiments/studying-apis` directory of the repository. To run the experiments, make sure to place the relevant traces in the corresponding experiment folder within this directory. For example, an example trace bitbrains-small is provided in the traces directory for your reference.

### Setup Traces
1. Clone the repository to your local machine.
2. Navigate to the `opendc-experiments/studying-apis` folder.
3. Place the relevant traces for the experiment you want to run in the corresponding experiment folder.

Here are the steps for proper trace placing:

1. Download the necessary traces from the following link: https://zenodo.org/record/7996316.
2. Store the downloaded traces in the directory structure: `opendc-experiments/studying-apis/<experiment>/src/main/resources/trace`.
3. Create a folder within the `trace` directory with a name corresponding to the source of the trace (e.g., `google`, `azure`, `bitbrains`).
4. Place the trace Parquet file inside the respective source folder.
5. Make sure to rename the trace file as `meta.parquet` when copying it.

These steps ensure that the traces are properly organized and available for the evaluation experiments using the OpenDC platform.

### Compile and Run
1. Open the terminal and navigate to the `opendc` project directory.
2. Run the command `./gradlew :opendc-experiments:studying-apis:<experiment>:experiment` to compile and run the desired experiment.

## Plotting Experiment Results

To plot the experiment results, please follow these steps:

1. Install the required Python packages by running the following command:
   ```
   pip install -r plot/script/requirements.txt
   ```

2. Ensure that all the necessary files for plotting are located under the `./plot` folder in the repository.

3. Set up the trace data by copying the corresponding trace files to the `./plot/trace/<dataset>` directory. Rename the trace file to `meta.parquet`. For example, if you are working with the Azure dataset, the path would be `./plot/trace/azure/meta.parquet`.

4. Prepare the input folder for the experiments you have run. Copy the experiment output to the path `./plot/input/<experiment>/<dataset>`. To locate the output of the experiments, you can find them under the following directory structure: opendc-experiments/studying-apis/<experiment>/output/<dataset>. Each experiment and dataset combination will have its own corresponding folder under the output directory. For instance, if you ran the migrations experiment using the Azure dataset, the path would be `./plot/input/migrations/azure`.

5. Choose one of the available scripts located in the `./plot/script` directory based on the experiment type. The available scripts are:
    - `migrations_plot_results.py`
    - `reservations_plot_results.py`
    - `metadata_plot_results.py`
    - `metadata_plot_preview.py`

6. Run the selected script with the desired dataset parameter. For example, to plot the results for the Azure dataset using the reservations experiment, run the following command:
   ```
   python3 ./plot/script/reservations_plot_results.py azure
   ```

7. The generated plots and visualizations will be available in the `./plot/output/<experiment>/<dataset>` directory.

Please note that the dataset parameter is mandatory when running the plotting script, as it determines which dataset the script will process.

Feel free to explore the generated plots and visualizations to analyze and interpret the experiment results.

That's it! You should now be able to plot your experiment results using the provided scripts and instructions.


## OpenDC
The evaluation experiments are performed using OpenDC, an open-source data center simulation platform. OpenDC allows for accurate and realistic simulations of data center environments, enabling thorough evaluation and analysis of various scheduling APIs and their impact on performance.

-----

OpenDC is a free and open-source platform for datacenter simulation aimed at both research and education.

![Datacenter construction in OpenDC](docs/images/screenshot-construction.png)

Users can construct datacenters (see above) and define portfolios of scenarios (experiments) to see how these
datacenters perform under different workloads and schedulers (see below).

![Datacenter simulation in OpenDC](docs/images/screenshot-simulation.png)

The simulator is accessible both as a ready-to-use website hosted by us at [opendc.org](https://opendc.org), and as
source code that users can run locally on their own machine, through Docker.

To learn more
about OpenDC, have a look through our paper [OpenDC 2.0](https://atlarge-research.com/pdfs/ccgrid21-opendc-paper.pdf)
or on our [vision](https://atlarge-research.com/pdfs/opendc-vision17ispdc_cr.pdf).

🛠 OpenDC is a project by the [@Large Research Group](https://atlarge-research.com).

🐟 OpenDC comes bundled
with [Capelin](https://repository.tudelft.nl/islandora/object/uuid:d6d50861-86a3-4dd3-a13f-42d84db7af66?collection=education)
, the capacity planning tool for cloud datacenters based on portfolios of what-if scenarios. More information on how to
use and extend Capelin coming soon!

### Documentation

The documentation is located in the [docs/](docs) directory and is divided as follows:

1. [Deployment Guide](docs/deploy.md)
1. [Architectural Overview](docs/architecture.md)
1. [Toolchain Setup](docs/toolchain.md)
1. [Research with OpenDC](docs/research.md)
1. [Contributing Guide](CONTRIBUTING.md)

## Contributing

Questions, suggestions and contributions are welcome and appreciated!
Please refer to the [contributing guidelines](CONTRIBUTING.md) for more details.

## License

This work is distributed under the MIT license. See [LICENSE.txt](/LICENSE.txt).
