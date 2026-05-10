FROM eclipse-temurin:11-jre
WORKDIR /app
COPY ingestion/target/ingestion-assembly.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
