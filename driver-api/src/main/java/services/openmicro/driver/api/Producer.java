package services.openmicro.driver.api;

public interface Producer {
    void publishEvent(long startTimeNS);
}
