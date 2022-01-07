package net.openhft.microservices.benchmark.driver.api;

/**
 * Generic interface of an event hander
 * @param <T> type of the event
 */
public interface EventHandler<T extends Event> {
    void event(T event);
}
