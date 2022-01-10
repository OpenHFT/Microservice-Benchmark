package services.openmicro.driver.api;

/**
 * Timestamps needed by the framework to determine the timings
 */
public interface Event {
    void sendingTimeNS(long sendingTimeNS);

    long sendingTimeNS();

    void transactTimeNS(long transactTimeNS);

    long transactTimeNS();
}
