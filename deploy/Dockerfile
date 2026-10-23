# Job image for a self-hosted Flink cluster: the same image runs the JobManager, the TaskManagers and the
# `flink run` client. Build the fat jar first: ./gradlew shadowJar
FROM flink:1.19-java17

# Checkpoints and savepoints go to S3-compatible storage (Garage): enable Flink's bundled S3 plugin.
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

COPY build/libs/real-time-alerts-*-all.jar /opt/flink/usrlib/real-time-alerts.jar
COPY examples /opt/flink/usrlib/examples
