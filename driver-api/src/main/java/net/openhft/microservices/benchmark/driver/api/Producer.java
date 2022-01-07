package net.openhft.microservices.benchmark.driver.api;

public interface Producer {
    void publishEvent(long startTimeNS);
}
