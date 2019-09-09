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
python captain.py label --files local_data/data.csv --sample all
```
You can specify as much files are you want. You can also make use of 
the wild card like that `local_data/*.csv`.
This can be executed multiple times without the need to restart or 
rebuild the services


*arguments*:
```
usage: label_experiment.py [-h] [--alpha ALPHA] [--fname FNAME]
                           [--sample {all,10}]
                           {start,results,show,collect,single}

Captain to look after the processes

positional arguments:
  {start,results,show,collect,single}
                        "start": To start the experiment "collect": To collect
                        the results from the running combine instances
                        "results": To compute the collected results (to be run
                        after the "collect" option) "show": To show the
                        computed results (to be run after the "results"
                        option) "single": To show the results for a single
                        file with a given alpha (to be run after the
                        "collect")

optional arguments:
  -h, --help            show this help message and exit
  --alpha ALPHA         The alpha to be used (only for single option)
  --fname FNAME         The file name the results will be computed for (only
                        for single option)
  --sample {all,10}     The sampling method
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
4. Run the labeling task `python label_experiment.py start --sample all` (note that this will 
use docker-compose and will startup the instances, automatically)
5. In another window, run this command `python label_experiment.py collect --sample all`. This 
will collect the data from the instances, so in case the experiment has been interrupted or
stopped, it will resume (to resume, start from step 4).
6. Once the experiment is done, you can compute the results `python label_experiment.py results --sample all` (it will fetch them from the combine instances) 
7. Show the scores `python label_experiment.py show --sample all` (precision, recall, and F1)

*note: for sample `all`, it will run normally, for sample `10`, it will take the top 10 values from each subject column only*

<!--
**TO BE CONTINUE**

### T2D-TAIPAN 
The T2D set used in the TAIPAN 
1. `cd experiments/taipan`
2. `python preprocessing.py` (you must have `wget` installed).
-->