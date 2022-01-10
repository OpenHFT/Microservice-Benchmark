package services.openmicro.driver.noop;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import services.openmicro.driver.api.Driver;
import services.openmicro.driver.api.Event;
import services.openmicro.driver.api.Producer;

import java.util.function.Consumer;

public class NoopDriver extends SelfDescribingMarshallable implements Driver {
    final Event event = new NoopEvent();

    @Override
    public Producer createProducer(Consumer<Event> eventConsumer) {
        return s -> {
            event.sendingTimeNS(s);
            event.transactTimeNS(System.nanoTime());
            eventConsumer.accept(event);
        };
    }
}
