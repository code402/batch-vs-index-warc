# batch-vs-index-warc

A benchmark to explore the speed of reading WARC entries in bulk vs individually.

```bash
mvn clean install assembly:single   # Build the JAR
```

```bash
NUM_RECORDS=100000 NUM_CORES=16 java -Dhttp.maxConnections=1000 -cp target/batch-vs-index-warc-1.0-SNAPSHOT-jar-with-dependencies.jar com.code402.Single

NUM_RECORDS=100000 NUM_CORES=16 java -Dhttp.maxConnections=1000 -cp target/batch-vs-index-warc-1.0-SNAPSHOT-jar-with-dependencies.jar com.code402.Batch
```
