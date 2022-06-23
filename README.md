# spark-ss-hudi-delta-poc
Spark Structure Streaming Source S3 Files or SNS SQS Stream and Sink S3 with format Parquet/Delta/Hudi

Please use below to compile succesfully.
Java 11
Spark 3.2
Scala 1.12

Please change <scope>provided</scope> to <scope>compile</scope> inside pom for local run.
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.version}</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.version}</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>
