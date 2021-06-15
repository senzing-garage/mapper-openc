# mapper-openc

## Overview

The Open Corporates mapper python scripts convert the Open Corporates company and officers located
[here](https://opencorporates.com/info/our-data//).  While you can search individual companies for free, you will need to purchase 
the regions you desire for loading into Senzing. You can purchase and download files of companies, their officers, and their additional 
(non-registered) addresses.

There are two python scripts in this project ...
- The [openc-companies.py](openc-companies.py) script maps the companies with their additional addresses if present. 
- The [openc-officers.py](openc-officers.py) script just maps the officers and relates them to their company.

Loading this data into Senzing requires additional features and configurations. These are contained in the
[openc-config-updates.g2c](openc-config-updates.g2c) file.

## Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Configuring Senzing](#configuring-senzing)
4. [Running the companies mapper](#running-the-companies-mapper)
5. [Running the officers mapper](#running-the-officers-mapper)

### Prerequisites

- python 3.6 or higher
- Senzing API version 2.1 or higher
- pandas (pip3 install pandas)

### Installation

Place the the following files on a directory of your choice ...

- [openc-files.json](openc-files.json)
- [openc-companies.py](openc-companies.py)
- [openc-officers.py](openc-officers.py)
- [openc-config-updates.g2c](openc-config-updates.g2c)

*You will need to update the [openc-files.json](openc-files.json) with the list and location of the files you downloaded from 
the Open Corporates website.  If the companies, officers or addresses files have been split, make sure you list them all under the associated file type as shown in the 
sample file included in this project.*

### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

From your Senzing project directory ...

```console
python3 G2ConfigTool.py <path-to-file>/openc_config_updates.g2c -f
```

This will step you through the process of adding the data sources, features, attributes and other settings needed to load this data into 
Senzing. After each command you will see a status message saying "success" or "already exists". For instance, if you run the script twice, 
the second time through they will all say "already exists" which is OK.

### Running the companies mapper

help:

```console
python3 openc-companies.py --help
usage: openc-companies.py [-h] [-i INPUT_FILE_SET] [-o OUTPUT_FILE_DIR] [-d DATA_SOURCE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE_SET, --input_file_set INPUT_FILE_SET
                        the name of the json file containing the list of open corporates csv files
  -o OUTPUT_FILE_DIR, --output_file_dir OUTPUT_FILE_DIR
                        the name of the output file directory
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the name of the data source code to use, defaults to: OPENC-COMPANY
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file

```

Typical use: 
```console
python3 openc-companies.py -i ./openc-files.json -o ./output/ -l output/companies-log.json
```

- The input file set should always be the openc-files.json that contains the file locations of the files you downloaded.
- The output file directory must be a directory, not a file.  There will be one json file output named after each company csv file
registered in the input file set.
- The log file is optional but gives you the statistics and examples of each attribute that was mapped.
- You can use the -d parameter to change the data source code the mapper assigns if you would rather use something other than the default.

*Please note that if you have downloaded the additional addresses file, the companies mapper will load them into into an sqlite database 
so they can be looked up and mapped for each company that has them. The sqlite database is created on the current working directory, so 
you must have write privileges and enought disk space to load them.*

### Running the officers mapper

help:

```console
python3 openc-officers.py --help
usage: openc-officers.py [-h] [-i INPUT_FILE_SET] [-o OUTPUT_FILE_DIR] [-d DATA_SOURCE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE_SET, --input_file_set INPUT_FILE_SET
                        the name of the json file containing the list of open corporates csv files
  -o OUTPUT_FILE_DIR, --output_file_dir OUTPUT_FILE_DIR
                        the name of the output file directory
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the name of the data source code to use, defaults to: OPENC-OFFICER
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

Typical use: 
```console
python3 openc-officers.py -i ./openc-files.json -o ./output/ -l output/officers-log.json
```

- The input file set should always be the openc-files.json that contains the file locations of the files you downloaded.
- The output file directory must be a directory, not a file.  There will be one json file output named after each officer csv file
registered in the input file set.
- The log file is optional but gives you the statistics and examples of each attribute that was mapped.
- You can use the -d parameter to change the data source code the mapper assigns if you would rather use something other than the default.


