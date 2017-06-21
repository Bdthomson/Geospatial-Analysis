
# CSE 512 - GeoSpatial Analysis - Data Monkeys

## Group Members

Alphabetical Order: 

Patrick Gaines

Spencer MK Smith

Fatima Naveed

Ryan Schachte

Blake Thomson

## Problem Definition

The 2016 Geographic Information Systems Cup problem definition can be found [here](http://sigspatial2016.sigspatial.org/giscup2016/problem).

## Setup

This spark implementation utilized 4 AWS machines in a cluster to solve the 2016 GIS Cup in 19.5 seconds.

To setup an AWS cluster of your own, please refer to my guide and culmination of resources [here](https://github.com/Bdthomson/ec2_spark_cluster_instructions).

# Yellow Taxi Heat Map using Getis-Ord Statistic

From the directions: The submitted jar will be invoked using the following syntax:

`./bin/spark-submit [spark properties] --class [submission class] [submission jar] [path to input]	
[path to output]`

## Building The Jar
In the project root, `mvn clean install` will place the compiled jar file in `target/`. Instructions to run it
are provided below.

## Spark Submit
Note: Make sure to specify the output file exactly, including the extension. We output file contents into whatever
file you specify, so if you want the outputs in `output.csv`, then you have to give `output.csv` as the final part
of the output argument. Examples are provided below.

General:

`$SPARK_BASE/bin/spark-submit 
--master [master_location] 
--class com.datamonkeys.phase3.Driver target/group6_phase3.jar 
[path_to_input] 
[path_to_output.csv]`

Example 1 (Local):

`$SPARK_BASE/bin/spark-submit 
--master local 
--class com.datamonkeys.phase3.Driver target/group6_phase3.jar 
"hdfs://localhost:9000/usr/ubuntu/yellow_cab_ten_thousand.csv" 
"hdfs://localhost:9000/path/to/output/group6_phase3_result.csv"`

Example 2 (EC2 Cluster):

`$SPARK_BASE/bin/spark-submit 
--master spark://ec2-35-164-195-55.us-west-2.compute.amazonaws.com:7077 
--class com.datamonkeys.phase3.Driver target/group6_phase3.jar 
"hdfs://localhost:9000/usr/ubuntu/yellow_cab_one_million.csv" 
"hdfs://localhost:9000/path/to/output/group6_phase3_result.csv"`

Output Location: You can output to the current directory by specifying `[path_to_output.csv]` as just the filename, like
this: `group6_phase3_result.csv`. If you want to output to a directory in HDFS, you must give the fully qualified path, like
this: `hdfs://localhost:9000/path/to/output/group6_phase3_result.csv`.

## Spark Properties
On our EC2 Spark Cluster, we have these 3 configuration properties set in our `$SPARK_BASE/conf/spark-env.sh` files, but it is most likely overkill for our implementation of MapReduce.
```bash
export SPARK_EXECUTOR_MEMORY=30g
export SPARK_EXECUTOR_CORES=24
export SPARK_DRIVER_MEMORY=30g
```
