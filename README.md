Performance loader
=========

Tool to create a distributed workload on Hadoop and Ozone clusters.

### How to run

In the project root directory

```
cp src/main/resources/loader.properties.template loader.properties
```

edit `loader.properties`

```
./gradlew clean build
java -jar ./build/libs/fat-perf-loader-1.0.25-fat.jar
```
