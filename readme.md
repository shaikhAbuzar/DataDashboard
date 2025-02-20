# True Beacon - Data Dashboard

The following project implements a data dashboard from [this](https://truebeacon.notion.site/Full-Stack-Coding-Assignment-2025-188f322ed20180ce86d1c810de260f20) project link.

The project focuses on the implementation of the multiple individual units to form a full stack data application which can be scaled easily while being easy to modify and change things without breaking existing processes.


## Structure of the Code Base
```bash
TrueBeacon
├── api
│   ├── __init__.py
│   └── main.py
├── dashboard
│   └── dashboard.py
├── data
│   ├── bhavcopy
│   │   ├── EODSNAPSHOT_04APR2022bhav.csv.zip
│   │   └── EODSNAPSHOT_05APR2022bhav.csv.zip
│   ├── db_data
│   └── tbt
│       ├── STOCK_TICK_04042022.zip
│       └── STOCK_TICK_05042022.zip
├── database
│   ├── __init__.py
│   └── dbutils.py
├── flow-diagram.png
├── ingestion
│   ├── __init__.py
│   └── ingester.py
├── readme.md
├── requirements.txt
├── sanity
│   ├── __init__.py
│   └── bhavcopy.py
├── Scripts
│   ├── db_install.sh
│   └── test.py
└── system_architecture.md
```
## Directories and File Descriptions
### Dir: `api`
The api directory hosts the file `main.py` which is the main script for running the FastAPI module and use the relevant API endpoints.

### Dir: `dashboard`
The dashboard contains the file `dashboard.py` which the core code base for running the streamlit based data dashboard to be used for interacting with the API endpoints and viewing the results.

### Dir: `data`
The directory hold all the data we want to store and access from here, including a path for mapping and storing persistent data for the database _(not recommended)_.

### Dir: `database`
The name can be a bit misleading here, but the `database` dir hold the utilities for interacting with different utilities, currently inside a single file but can be seggregated to their independent components while keeping the core `DBInterface` in a standalone file.

### File: `flow-diagram.png`
The file for the flow diagram of the entire application pipeline depicting how each component interacts with each other, for detail explanation refer [`system_architecture.md`](system_architecture.md).

### Dir: `ingestion`
The ingestion dir contains the data ingestion pipeline for inserting the data into the DB, this file internally calls the `dbutil.py` to get the correct database module and perform the insertion.

### File: `readme.md`
This file that you are reading for understanding all the other files.

### Dir: `sanity`
The sanity directory contains the sanity module which is used to access the `bhavcopy.py` file of the sanity module to perform the bhavcopy checks.

### Dir: `Scripts`
This is where adhoc scripts and `test.py` file is for playing around with ideas for improving or implementing stuff. This also has the `db_install.sh` script which has a single command for starting a postgres db docker instance.

### File: `system_architecture.md`
This is the file that explains the whole system architecture of the application in detail.

## Setting it up for local

> **NOTE:** This repo assumes you have docker and python3.8+ installed in your system, if not please install them as the prerequisite step.

1. Clone this entire repo using the below command, and move inside the repo.
```bash
$ git clone <url of this repo>

$ cd TrueBeacon
```

2. Make sure to install and run the postgres script, the one name [`db_install.sh`](Scripts/db_install.sh). Or alternatively you can run the command in the script directly on the command line.
```bash
# 1. Run the script 
$ bash Scripts/db_install.sh

# 2. Run the command directly from the TrueBeacon directory
$ docker run --name my_postgres -e POSTGRES_PASSWORD=mysecretpassword  -v $PWD/Data/db_data:/var/lib/postgresql/data -d -p 5432:5432 postgres
```
3. Create a virtual environment for you python library, so that dependencies for this environment is not impacting any other project you may have running, command to do the same is as below. Once the environment is created, activate it.

```bash
# 1. Create Virtual Environment named tbvenv
$ python3.8 -m venv tbvenv

# 2. Activate the virtual environment
$ source tbvenv/bin/activate
```

4. After you are in the virtual environment install the dependencies from the [`requirements.txt`](requirements.txt) file using the following command.
```bash
$ pip install -r requirements.txt
```

5. Once you have installed the same you are going to insert the data available in the [`data/tbt/`](data/tbt/) directory. We have the data for the dates `2022-04-04` and `2022-04-05`, to insert the data we will use the [`ingester.py`](Ingestion/ingester.py) file, run the following commands to begin the insertion.
```bash
$ python Ingestion/ingestor.py 2022-04-04

$ python Ingestion/ingestor.py 2022-04-05
```

6. After the data insertion is done we will start the API endpoints by running the following command.
```bash
$ fastapi dev api/main.py
```

7. After the API server has been inserted and open a new terminal and navigate till the `TrueBeacon` directory (the path where you have cloned this repo). Once inside the repo activate the virtual environment as mentioned in the second command of `Step 3`, after that run the following command to run the streamlit dashboard.
```bash
$ streamlit run dashboard/dashboard.py
```

8. You can access both the API endpoints from their individual command runs and should be able to access the same via the browser (for dashboard and api) or any other tool for working with APIs

