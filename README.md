# mapper-openc

## Overview

The Open Corporates mapper python scripts convert the Open Corporates company and officers located
[here](https://opencorporates.com/info/our-data//).  While you can search individual companies for free, you will need to purchase 
the regions you desire for loading into Senzing. You can purchase and download files of companies, their officers, and their additional 
(non-registered) addresses.

There are two python scripts in this project ...
- The [openc-load-childb.py](openc-load-childb.py) script creates the child file database for additional addresses, aliases and identifiers.
- The [openc-companies.py](openc-companies.py) script maps the companies with their additional addresses if present. 
- The [openc-officers.py](openc-officers.py) script just maps the officers and relates them to their company.

Loading this data into Senzing requires additional features and configurations. These are contained in the
[openc-config-updates.g2c](openc-config-updates.g2c) file.

## Contents

1. [Prerequisites](#prerequisites)
2. [Download Open Corporates files](#download-open-corporates-files)
3. [Installation](#installation)
4. [Configuring Senzing](#configuring-senzing)
5. [Create the child database](#create-the-child-database)
6. [Running the companies mapper](#running-the-companies-mapper)
7. [Running the officers mapper](#running-the-officers-mapper)

### Prerequisites

- python 3.6 or higher
- Senzing API version 2.1 or higher
- pandas (pip3 install pandas)
- orjson (pip3 install orjson)

### Download Open Corporates files

When you purchase Open Corporates bulk data, you will be given instructions for how to download their files.  These are the expected files ...

These are the primary files:
- **companies.csv.gz** This is the list of companies.
- **officers.csv.gz** This is the list of officers for the companies.

These are child files containing additional information for companies:
- **alternative_names.csv.gz** This is a list of additional names for the companies.
- **non_reg_addresses.csv.gz** This is a list of additional addresses for the companies.
- **additional_identifiers.csv.gz** This is a list of tax and other identifiers for the companies.

Place all these files on a single directory refered to as the "input" directory below.

### Installation

Place the the following files on a directory of your choice ...

- [openc-config-updates.g2c](openc-config-updates.g2c)
- [openc-load-childb.py](openc-load-childb.py)
- [openc-companies.py](openc-companies.py)
- [openc-officers.py](openc-officers.py)

### Configuring Senzing

*Note:* This only needs to be performed one time! In fact you may want to add these configuration updates to a master configuration file for all your data sources.

From your Senzing project directory ...

```console
python3 G2ConfigTool.py <path-to-file>/openc_config_updates.g2c -f
```

This will step you through the process of adding the data sources, features, attributes and other settings needed to load this data into 
Senzing. After each command you will see a status message saying "success" or "already exists". For instance, if you run the script twice, 
the second time through they will all say "already exists" which is OK.


### Create the child database

*This step must be performed before running the mappers!*

```console
python3 openc-load-childb.py --help
usage: openc-load-childb.py [-h] [-i INPUT_FILE_DIR] [-c CHILD_DATABASE_NAME]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE_DIR, --input_file_dir INPUT_FILE_DIR
                        the name of the open corporates csv file directory
  -c CHILD_DATABASE_NAME, --child_database_name CHILD_DATABASE_NAME
                        the name of the database file to create
```

Typical use:
```console
python3 openc-load-childb.py -i ./input -c ./input/child.db
```

- The -i should be the directory where you downloaded the Open Corporates data files.
- The -c should be where you want the sqlite child database to be written.  Ideally, you would place it on the same directory.


### Running the companies mapper

```console
python3 openc-companies.py --help
usage: openc-companies.py [-h] [-i INPUT_FILE_DIR] [-o OUTPUT_FILE_DIR] [-c CHILD_DATABASE_NAME] [-d DATA_SOURCE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE_NAME, --input_file_name INPUT_FILE_NAME
                        the name of an open corporates csv file for companies
  -o OUTPUT_FILE_NAME, --output_file_name OUTPUT_FILE_NAME
                        the name of the output file
  -c CHILD_DATABASE_NAME, --child_database_name CHILD_DATABASE_NAME
                        the name of the child database created in the prior step
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the name of the data source code to use, defaults to: OPENC-COMPANY
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
  -w MAX_WORKERS, --max_workers MAX_WORKERS
                        defaults to the number of system processors, may need to reduce if running other things at same time
```

Typical use: 
```console
python3 openc-companies.py -i ./input/companies.csv.gz -c ./input/child.db -o ./output/companies.json -l output/company-mapping-log.json
```

- The -i is location of the Open Corporates companies data file.
- The -c is where the child database you created in the prior step is located.
- The -o is where you want the mapped file to be written.
- The -l is an optional log file that contains mapping stats for your review.


### Running the officers mapper

```console
python3 openc-officers.py --help
usage: openc-officers.py [-h] [-i INPUT_FILE_SET] [-o OUTPUT_FILE_DIR] [-d DATA_SOURCE] [-l LOG_FILE]

optional arguments:
  -h, --help            show this help message and exit
  -i INPUT_FILE_NAME, --input_file_name INPUT_FILE_NAME
                        the name of an open corporates csv file for officers
  -o OUTPUT_FILE_NAME, --output_file_name OUTPUT_FILE_NAME
                        the name of the output file
  -t TEMP_DATABASE_NAME, --temp_database_name TEMP_DATABASE_NAME
                        the name for the temporary database required to de-dupe officers
  -d DATA_SOURCE, --data_source DATA_SOURCE
                        the name of the data source code to use, defaults to: OPENC-OFFICER
  -l LOG_FILE, --log_file LOG_FILE
                        optional name of the statistics log file
```

Typical use: 
```console
python3 openc-officers.py -i ./input/officers.csv.gz -o ./output/officers.json -t ./input/temp.db -l output/officers-log.json
```

- The -i is the location of the Open Corporates officers data file.
- The -o is where you want the mapped file to be written.
- The -t is for the temporary sqlite database used to de-dupe officers.
- The -l is an optional log file that contains mapping stats for your review.

*Note:* The temporary database file will be overwritten if exists! You can delete it manually after the run to save disk space.
