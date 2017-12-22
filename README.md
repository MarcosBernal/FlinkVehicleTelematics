Limited program of Flink using Java 8 and maven 3.7 to detect vehicles illegal speed and movements.
 The data is obtained by reading a file. Small sample in the traffic_data file.

## Requirements

- flink >= 1.3.2
- java >= 1.8
- maven >= 3.7.0

Tested with ubuntu 16.04

## Steps to use

### With flink installed

- Start flink process: `i.e. /opt/flink-1.3.2/bin/start-cluster.sh`
- Compile the source code: `mvn clean install`
- Run processes:
    -   `mvn exec:java -Dexec.mainClass="master2017.flink.VehicleTelematics" input_file_path output_file_path`   

### With docker

- Move to the repository folder: `cd FlinkVehicleTelematics`
- Build the custom image: `sudo docker build -t ubuntu-flink .`
    - You can use other images (with docker pull) such as the [official one](https://hub.docker.com/r/_/flink/)
- Start the container: `sudo docker run -it --rm -v $(pwd):/home/dev -p 8081:8081 ubuntu-flink`
- Create the jar file: `mvn clean package -Pbuild-jar`
- Run the command inside the container (flink cluster is already running):
    - `flink run -p 10 -c master2017.flink.VehicleTelematics target/FlinkVehicleTelematics-1.0-SNAPSHOT.jar traffic_data traffic_output`


## Development 

Check the official docs about the API

https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html