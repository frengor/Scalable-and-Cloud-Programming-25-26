# Scalable and Cloud Programming project 2025-26

Distributed earthquake co-occurrence analysis using Apache Spark for the course of Scalable and Cloud Programming at the University of Bologna A.A. 2025/2026.

## Build and run locally

```shell
sbt clean package

spark-submit --class ScalableCourseProject \
  target/scala-2.12/scalablecourseproject_2.12-1.0.0.jar \
  dataset-earthquakes-trimmed.csv \
  [numPartitions] \
  [numWorkers]
```

## CLI parameters

* `inputFile`: path to the dataset file

* `numPartitions`: number of partitions per core (spark's `defaultParallelism` value). Defaults to 8

* `numWorkers`: number of workers in the cluster. Defaults to 1

The total number of partitions used is given by `numPartitions` * `numWorkers` * `spark.sparkContext.defaultParallelism`.

## Run on Dataproc

Upload the dataset and the compiled binary (can be found in `target/scala-2.12/scalablecourseproject_2.12-1.0.0.jar`) to a bucket.

Then, create a cluster with the desired number of workers

```shell
gcloud dataproc clusters create <name> \
  --region=<region> \
  --num-workers <n> \
  --master-boot-disk-size 240 \
  --worker-boot-disk-size 240 \
  --master-machine-type=n2-standard-4 \
  --worker-machine-type=n2-standard-4
```

and submit a job

```shell
gcloud dataproc jobs submit spark \
  --cluster=<name> \
  --region=<region> \
  --jar=gs://<bucketName>/scalablecourseproject_2.12-1.0.0.jar \
  -- gs://<bucketName>/dataset-earthquakes-full.csv \
  [numPartitions] \
  [numWorkers]
```

Remember to grant the cluster the `dataproc.worker` role 

```shell
gcloud projects add-iam-policy-binding <projectId> \
  --member="serviceAccount:<projectNumber>-compute@developer.gserviceaccount.com" \
  --role="roles/dataproc.worker"
```
