package com.bigdata2026.ingestion;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        System.out.println(
            "[ingestion] Java Kafka producer skeleton. " +
            "Wire KafkaProducer<String,String> against bootstrap.servers=localhost:9092.");
    }
}
