package services.openmicro.driver.kafka;

import services.openmicro.driver.api.EventHandler;

public class EventMicroservice implements EventHandler<ChronicleEvent> {
    final transient EventHandler out;

    public EventMicroservice(EventHandler out) {
        this.out = out;
    }

    @Override
    public void event(ChronicleEvent event) {
        event.transactTimeNS(System.nanoTime());
        out.event(event);
    }
}
