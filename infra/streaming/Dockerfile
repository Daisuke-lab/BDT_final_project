FROM eclipse-temurin:11-jre
WORKDIR /app
COPY streaming/target/scala-2.12/streaming-assembly.jar app.jar
# Spark is bundled in the fat JAR; local[*] mode needs no external spark-submit.
ENTRYPOINT ["java", "-jar", "app.jar"]
