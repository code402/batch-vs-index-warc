# batch-vs-index-warc

_See the blog post: [S3 Throughput: Scans vs Indexes](https://code402.com/blog/s3-scans-vs-index/)._

A benchmark to explore the speed of reading WARC entries in bulk vs individually.

```bash
mvn clean install assembly:single   # Build the JAR
```

```bash
NUM_RECORDS=100000 NUM_CORES=16 java -Xmx20g -Dhttp.maxConnections=1000 -cp target/batch-vs-index-warc-1.0-SNAPSHOT-jar-with-dependencies.jar com.code402.Single

NUM_RECORDS=100000 NUM_CORES=16 java -Xmx20g -Dhttp.maxConnections=1000 -cp target/batch-vs-index-warc-1.0-SNAPSHOT-jar-with-dependencies.jar com.code402.Batch
```
