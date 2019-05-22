# tada-gam
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


