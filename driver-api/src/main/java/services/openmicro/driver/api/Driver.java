package services.openmicro.driver.api;

import java.util.function.Consumer;

public interface Driver {

    Producer createProducer(Consumer<Event> event);

    /**
     * Initialise the driver
     */
    default void init() {
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
}
