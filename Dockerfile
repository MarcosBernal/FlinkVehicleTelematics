# Flink 1.3.2, hadoop and Java8
#
# VERSION               0.3

FROM ubuntu:xenial

USER root

# Install prerequisites
RUN apt-get update
RUN apt-get install -y software-properties-common

# Install java8
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java8-installer

RUN apt-get install -y maven

RUN wget -O flink http://apache.rediris.es/flink/flink-1.3.2/flink-1.3.2-bin-hadoop27-scala_2.11.tgz
RUN tar -xvzf flink -C /opt

# Adding flink to path
ENV PATH=$PATH:/opt/flink-1.3.2/bin

RUN mkdir /home/dev
WORKDIR /home/dev

# TaskManagers Data
EXPOSE 6121

# TaskManagers RPC port
EXPOSE 6122

# JobManager RPC port
EXPOSE 6123

# PORT 8081 need to be port in docker run to access the dashboard

# Adding last utilities
RUN apt-get install -y nano

# Setting 10 workers and default parallelism to 10 in flink cluster
COPY flink-conf.yaml /opt/flink-1.3.2/conf/flink-conf.yaml

CMD start-cluster.sh > /dev/null & echo "Started flink cluster" & /bin/bash

# sudo docker build -t ubuntu-flink .
# sudo docker run -it --rm -v $(pwd):/home/dev -p 8081:8081 ubuntu-flink
# flink run -p 1 -c master2017.flink.VehicleTelematics target/FlinkVehicleTelematics-1.0-SNAPSHOT.jar traffic_data traffic_output
# sudo docker run --rm -t -v $(pwd):/home/dev flink flink run -c master2017.flink.VehicleTelematics /home/dev/target/FlinkVehicleTelematics-1.0-SNAPSHOT.jar /home/dev/traffic_data /home/dev/traffic_output