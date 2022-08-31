package services.openmicro.driver.api;

import java.util.function.Consumer;

public interface Driver extends AutoCloseable {

    Producer createProducer(Consumer<Event> event);

    /**
     * Initialise the driver
     */
    default void init(TestMode testMode) {
    }

    /**
     * Start the service
     */
    default void start() {
    }

    /**
     * Stop the service and close any connections/resources
     */
    default void close() {
    }

    default int warmup() {
        return 500_000;
    }

    default int window() {
        return 1000;
    }
}
