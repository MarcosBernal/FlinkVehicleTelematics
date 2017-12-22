Limited program of Flink using Java 8 and maven 3.7 to detect vehicles illegal speed and movements.
 The data is obtained by reading a file. Small sample in the traffic_data file.

## Requirements

- flink >= 1.3.2
- java >= 1.8
- maven >= 3.7.0

Tested with ubuntu 16.04

## Steps to use

- Start flink process: `i.e. /opt/flink-1.3.2/bin/start-cluster.sh`
- Run processes:
    -   `mvn exec:java -Dexec.mainClass="master2017.flink.VehicleTelematics" input_file_path output_file_path`   



## Development 

Check the official docs about the API

https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html