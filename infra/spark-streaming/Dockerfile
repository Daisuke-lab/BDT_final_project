FROM eclipse-temurin:11-jre-jammy

WORKDIR /app

COPY spark-streaming/target/scala-2.13/spark-streaming-assembly.jar app.jar

ENV HBASE_ZOOKEEPER_QUORUM=hbase
ENV HBASE_ZOOKEEPER_PORT=2182

ENTRYPOINT ["java", "-cp", "app.jar", "com.bigdata2026.spark.TrendingJob"]
