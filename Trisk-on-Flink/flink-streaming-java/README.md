# Control Plane Development Guide

## Compile

> For project compiling, highly recommand compile the release-1.10 flink project first and compile our modified modules one by one.

The compilng order for modules we modified:

1. Flink-core
2. Flink-filesystems
3. Flink-runtime
4. Flink-streaming-java

Run [compile.sh](../compile.sh) to compile the modules one by one:

```bash
cd flink-core || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-filesystems || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-runtime || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
cd ../flink-streaming-java || exit
mvn clean install -Dcheckstyle.skip -DskipTests -Dmaven.javadoc.skip
```

## Run Entrypoint on IntelliJ IDEA

1. Open the [StandaloneSessionStreamManagerEntrypoint.java](src/main/java/org/apache/flink/streaming/controlplane/entrypoint/StandaloneSessionStreamManagerEntrypoint.java)

2. Cilck the Run Button

3. Edit the configuration

   ![image-20200702202006452](https://img-upic.oss-accelerate.aliyuncs.com/uPic/2020-07/ZnhRdY.png)

   ![image-20200702203005646](https://img-upic.oss-accelerate.aliyuncs.com/uPic/2020-07/WteRWl.png)

   1. Set the VM options

      ```
      -Xms1024m
      -Xmx1024m
      -Dlog.file=flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/log/flink-dr.fat-streammanager-standalonesession.local.log
      -Dlog4j.configuration=file:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/conf/log4j.properties
      -Dlogback.configurationFile=file:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/conf/logback.xml
      -classpath
      :flink-runtime/target/flink-runtime_2.11-1.10-SNAPSHOT.jar:flink-core/target/flink-core-1.10-SNAPSHOT.jar:flink-streaming-java/target/flink-streaming-java_2.11-1.10-SNAPSHOT.jar:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/lib/flink-table-blink_2.11-1.10-SNAPSHOT.jar:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/lib/flink-table_2.11-1.10-SNAPSHOT.jar:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/lib/log4j-1.2.17.jar:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/lib/slf4j-log4j12-1.7.15.jar:flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/lib/flink-dist_2.11-1.10-SNAPSHOT.jar:::
      ```

   2. Set program arguments

      ```
      --configDir
      flink-dist/target/flink-1.10-SNAPSHOT-bin/flink-1.10-SNAPSHOT/conf
      --executionMode
      cluster
      ```

   3. Remove the before launch build actions

4. Click the run button again

   If you see the output below:

   ```
   bind address for sm dispatcher rest endpoint: null bind port: 8520
   ```

   Then, every thing is alright!



