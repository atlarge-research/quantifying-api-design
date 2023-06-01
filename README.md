<a href="https://opendc.org/">
    <img src="https://opendc.org/img/logo.png" alt="OpenDC logo" title="OpenDC" align="right" height="100" />
</a>

# Evaluation of Datacenter Scheduler Programming Abstractions

This repository contains the evaluation code and experiments written in Kotlin for the research work titled "The Cost of Simplicity: Understanding Datacenter Scheduler Programming Abstractions" presented at the Middleware 2023 conference. The work investigates the performance impact of various datacenter scheduler programming abstractions.

[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](/LICENSE.txt)
[![Documentation](https://img.shields.io/badge/docs-master-green.svg)](./docs)
[![GitHub release](https://img.shields.io/github/release/atlarge-research/opendc)](https://github.com/atlarge-research/opendc/releases)
[![Build](https://github.com/atlarge-research/opendc/actions/workflows/build.yml/badge.svg)](https://github.com/atlarge-research/opendc/actions/workflows/build.yml)

## Getting Started

The code for all the specific experiments can be found in the opendc-experiments/scheduling-apis directory of the repository. To run the experiments, make sure to place the relevant traces in the corresponding experiment folder within this directory. For example, an example trace bitbrains-small is provided in the traces directory for your reference.

### Prerequisites
- Kotlin and Gradle should be installed on your machine.

### Setup Traces
1. Clone the repository to your local machine.
2. Navigate to the `opendc-experiments/scheduling-apis` folder.
3. Place the relevant traces for the experiment you want to run in the corresponding experiment folder.

### Compile and Run
1. Open the terminal and navigate to the `opendc` project directory.
2. Run the command `./gradlew :opendc-experiments:<experiment>:experiment` to compile and run the desired experiment.

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
