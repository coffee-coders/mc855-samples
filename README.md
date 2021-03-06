# samples

## Run hadoop using docker
```shell
docker run --name hadoop -p 8088:8088 -p 50070:50070 -it sequenceiq/hadoop-docker:2.7.0 /etc/bootstrap.sh -bash
```

# Build project
```shell
mvn clean compile package
```

# Copy project do container
```shell
docker ps # discover the container_id

docker cp target/mc855-sample-*.jar hadoop:/sample1.jar
```

## Run samples
Samples:
 - word-count
 - letter-count
 - letter-frequency-count

```shell
cd /usr/local/hadoop/bin
./hadoop jar /sample1.jar letter-frequency-count input output
./hdfs dfs -cat output/part-r-00000 | awk  '{print $2 " " $1}' | sort -n
```

## Run map-viewer
Map viewer creates circles on map based on the data in the [data.json](map-viewer/data.json) file.

The map can be accessed on localhost:8080. To start the server run:
```shell
node server.js
```
