```shell
sbt clean package

spark-submit --class ProgettoScalable target/scala-2.12/progettoscalable_2.12-0.1.0-SNAPSHOT.jar dataset-earthquakes-trimmed.csv
```