# samples

## Run hadoop using docker
```shell
docker run -p 8088:8088 -p 50070:50070 -it sequenceiq/hadoop-docker:2.7.0 /etc/bootstrap.sh -bash
```

# Build project
```shell
mvn clean compile package
```

# Copy project do container
```shell
docker ps # discover the container_id

docker cp target/mc855-sample-*.jar <container_id>:/sample1.jar
```

## Run samples
Samples:
 - word-count
 - letter-count
 - letter-frequency-count

```shell
bin/hadoop jar /sample1.jar letter-frequency-count input output
bin/hdfs dfs -cat output/part-r-00000 | awk  '{print $2 " " $1}' | sort -n
```
