# ChargePointDataset

This repository contains the ChargePointDataset, a dataset for electric vehicle (EV) charging stations. The dataset is collected from ChargePoint cloud and can be used for research, analysis.

## Prerequisites

Before getting started, make sure you have the following prerequisites installed:

- Python (Version 3.10)
- pip (Version 22)

## Installation

To run the scripts, you need to install the required libraries listed in the [requirements.txt](requirements.txt) file. You can do this by running the following command:

```shell
pip install -r requirements.txt
```

## Start the daemon process

To start the daemon process for updating the dataset, you can run the following command:

```shell
python3 update_worker.py
```

To start the daemon process for uploading the dataset, you can run the following command:

```shell
python3 upload_worker.py
```

**Note:** *API keys and secrets have been removed from the [config.ini](config.ini) file.*

## Analysis reproduction
To reproduce the analytical results presented in this paper, you can run the script provided in the [chargepoint_analysis.ipynb](analysis/chargepoint_analysis.ipynb) in the module `analysis`. This script has been designed to ensure unbiased outcomes. In order to facilitate reproducibility, a data file [Sessions.csv](analysis/Sessions.csv) is provided. This file contains the data used during the paper writing process and serves as the dataset for performing computations. By utilizing this data file and executing the script, you can replicate the analytical processes described in the paper and obtain consistent results.
