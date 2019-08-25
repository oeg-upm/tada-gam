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
## Subject Column Detection
### T2Dv2
1. Download the data from the official [website](http://webdatacommons.org/webtables/goldstandard.html)
2. Locate the downloaded data into `experiments/t2dv2/data`
3. Go to the experiment directory `cd experiments/t2dv2`
4. Run the labeling task `python label_experiment.py start` (note that this will 
use docker-compose and will startup the instances, automatically)
5. In another window, run this command `python label_experiment.py collect`. This 
will collect the data from the instances, so in case the experiment has been interrupted or
stopped, it will resume (to resume, start from step 4).
6. Once the experiment is done, you can compute the results `python label_experiment.py results` (it will fetch them from the combine instances) 
7. Show the scores `python label_experiment.py show` (precision, recall, and F1)

<!--
**TO BE CONTINUE**

### T2D-TAIPAN 
The T2D set used in the TAIPAN 
1. `cd experiments/taipan`
2. `python preprocessing.py` (you must have `wget` installed).
-->