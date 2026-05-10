package com.bigdata2026.ingestion;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws InterruptedException {
        // Delegate to FakeGitHubProducer for now.
        // To wire in real GitHub data: delete FakeGitHubProducer.java and replace this call.
        FakeGitHubProducer.run();
    }
}
