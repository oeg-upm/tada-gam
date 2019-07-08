# tada-gam

[![Build Status](https://semaphoreci.com/api/v1/ahmad88me/tada-gam/branches/master/badge.svg)](https://semaphoreci.com/ahmad88me/tada-gam)
[![codecov](https://codecov.io/gh/oeg-upm/tada-gam/branch/master/graph/badge.svg)](https://codecov.io/gh/oeg-upm/tada-gam)


A scalable version of tada entity using the MapReduce framework

# Usage
To use this tool, we need to talk with the `captain.py`. It manages the 
other resources and assign tasks and data. Although it can be done
directly, but you need to understand how the flow works.

## Step1: Startup the services
```
python captain.py up --services score=3 combine=2
```
In this command we are running 3 instances of the `score` service and
2 instance of `combine`. You can adjust that to meet your needs 

## Step2: Label columns
```
python captain.py label --files local_data/data.csv
```
You can specify as much files are you want. You can also make use of 
the wild card like that `local_data/*.csv`.
This can be executed multiple times without the need to restart or 
rebuild the services


*arguments*:
```
usage: captain.py [-h] [--files FILES [FILES ...]] [--slicesize SLICESIZE]
                  [--services SERVICES [SERVICES ...]]
                  {label,ports,up}

Captain to look after the processes

positional arguments:
  {label,ports,up}      What action you like to perform

optional arguments:
  -h, --help            show this help message and exit
  --files FILES [FILES ...]
                        The set of file to be labeled
  --slicesize SLICESIZE
                        The max number of elements in a slice
  --services SERVICES [SERVICES ...]
                        The names of the services
```

# requirements
* `docker`
* `docker-compose`
* `python 2.7`

# To update submodules
```
git submodule foreach git pull origin master
```
[source](https://stackoverflow.com/questions/5828324/update-git-submodule-to-latest-commit-on-origin)


# To run the experiments
## T2Dv2
1. Download the data from the official [website](http://webdatacommons.org/webtables/goldstandard.html)
2. Locate the downloaded data into `experiments/t2dv2/data`
