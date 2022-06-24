Spark Structure Streaming on Data Lake
===================================== 
This is a Spark Structure Streaming Solution which uses source either
S3 Bucket Location or a SQS Name and Sink as S3 with formats can be any Parquet or Delta or Hudi


Details
-------
We need AWS S3 Account and S3 buckets. Also if you want to read from SQS (S3 files location metadata) instead of files directly from S3, please create SNS+SQS and record SQS queue name.
1. Source is S3
   1. S3 listing
   2. SNS SQS
2. Sink is S3 with formats can be
   1. Hudi
   2. Delta
   3. just simple Parquet


How to run in local mode?
-------
### Dependencies on Mac
1. Java 11
2. Scala 2.12
3. Spark 3.2
4. Intellij
5. Maven 3.8.6

### Data to use: 
1. spark-ss-hudi-delta-poc/data/unique_srno/*.csv
2. Upload these files to an S3 bucket.

### Build Locally
`1. git clone git@github.com:bktechmission/spark-ss-hudi-delta-poc.git`

`2. cd spark-ss-hudi-delta-poc/; mvn clean install`

`3. Open Intellij, point to pom.xml in the project dir`

### Run Locally in Intellij
**3 Files to Run**
1. **Parquet write:** Right click and run S3ToParquetStreamJob
2. **Hudi write:** Right click and run S3ToHudiOptStreamJob
3. **Delta write:** Right click and run S3ToDeltaStreamJob
Program Argument: local

**NOTE:**
1. Please make sure to update application.conf with your development.normv2.* settings like s3 paths and access/secret keys.
2. There is also SQS equivalent for above 3 files, if you want to run through SQS use instead of Plain S3 Directory Listing.


### Run Locally in CLI
`1. cd spark-ss-hudi-delta-poc/`

`2. spark-submit --class org.apache.spark.sql.streaming.jobs.S3ToHudiOptStreamJob target/normv2-poc-1.0-SNAPSHOT.jar local`

**Similar spark submit for classes S3ToDeltaStreamJob and S3ToParquetStreamJob**


### How to run on EMR mode?
**Parquet write:**
`spark-submit --master yarn --deploy-mode client --num-executors 12 --executor-memory 1g --driver-memory 1g --executor-cores 2 --conf spark.driver.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/* --conf spark.executor.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/* --class org.apache.spark.sql.streaming.jobs.S3ToParquetStreamJob s3://bhupis3test1/normv2-poc-1.0-SNAPSHOT.jar emr`

**Hudi write:**
`spark-submit --master yarn --deploy-mode client --num-executors 12 --executor-memory 1g --driver-memory 1g --executor-cores 2 --conf 'spark.driver.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --conf 'spark.executor.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --class org.apache.spark.sql.streaming.jobs.S3ToHudiOptStreamJob --packages 'org.apache.hudi:hudi-spark3.2-bundle_2.12:0.11.1' --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' s3://bhupis3test1/normv2-poc-1.0-SNAPSHOT.jar emr`

**Delta write:**
`spark-submit --master yarn --deploy-mode client --num-executors 12 --executor-memory 1g --driver-memory 1g --executor-cores 2 --conf 'spark.driver.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --conf 'spark.executor.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --class org.apache.spark.sql.streaming.jobs.S3ToDeltaStreamJob --packages 'io.delta:delta-core_2.12:1.2.1' --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' s3://bhupis3test1/normv2-poc-1.0-SNAPSHOT.jar emr`

### Readers
Just to check counts matches with Files on Local Disk and S3 written files.
1. DiskBasedTransformJob: Read files from Disk on Mac to validate the true counts
2. S3HudiBatchReader: Read files from Hudi and Delta S3 folders

### Perf Testing
1. For Perf Testing I used files from spark-ss-hudi-delta-poc/data/retail-data/*.csv, which are 305 files, please upload all of them to a clean S3 bucket. 
2. maxFilesPerTrigger: 100 inside your application.conf
3. Follow run on EMR steps for all 3 formats.

**Result:**
1. Parquet is 8x Faster than Hudi
2. Delta is 2x Faster than Hudi


### Fault Testing
1. I ran code inside Intellij.
2. Data used: spark-ss-hudi-delta-poc/data/unique_srno/*.csv
   1. Total rows 31102 
   2. Total Stock Codes agg_df.count: 2657 distinct:2657
3. Run all 3 one at a time S3ToParquetStreamJob, S3ToHudiOptStreamJob, S3ToDeltaStreamJob
4. Make sure to stop and start Spark Jobs multiple times.
5. maxFilesPerTrigger: 5 inside your application.conf

**For Hudi Data Duplication Repro:** 
1. Stop S3ToHudiOptStreamJob whenever you see on Intellij console logs 'INFO HoodieStreamingSink: Micro batch id='
2. That makes .hoodie/ folder contains commit files but not in Spark checkpoint/commit/ folder 
3. By doing this we will see lot more commits than commits in Spark checkpoint/commit/
NOTE: Hudi PrimaryKey is UUID, which is getting generated in S3ToHudiOptStreamJob, so technically each Stop/Start will generate a different UUID

I did not see other 2 format Duplicating Data. Only Hudi was not doing Exactly Once if UUID is generated inside Spark Code.

**Result:**
**1. S3ToParquetStreamJob**

**Repro Fault:** Kill job when ._spark_metadata/ has commit and no commit in checkpoint/commit/

**Readers Count:**

`parq_df.count 31102`

`agg_parq.count: 2657 distinct:2657`

`Total parquet show time: 23seconds`

**2. S3ToDeltaStreamJob**
**Repro Fault:** Kill job when _delta_log/ has .json or checkpoint/commit/ just started writing commit
   
**Readers Count:**

`delta_df.count 31102`

`agg_delta.count: 2657 distinct:2657`

`Total delta show time: 35seconds`

**3. S3ToHudiOptStreamJob**

**Repro Fault:** Kill job when .hoodie has commit and no commit in checkpoint/commit/
   
**Readers Count:**

`hudi_df.count 57834`

`agg_hd.count: 2657 distinct:2657`      
`Distinct StockCode remain same, that means we added more records`

`Total hudi show time: 48seconds`

### On EMR Reader Run
`spark-submit --master yarn --deploy-mode client --num-executors 12 --executor-memory 1g --driver-memory 1g --executor-cores 2 --conf 'spark.driver.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --conf 'spark.executor.extraClassPath=/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*' --packages io.delta:delta-core_2.12:1.2.1 --conf 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension' --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog' --class org.apache.spark.sql.streaming.jobs.S3HudiBatchReader s3://bhupis3test1/normv2-poc-1.0-SNAPSHOT.jar emr`



