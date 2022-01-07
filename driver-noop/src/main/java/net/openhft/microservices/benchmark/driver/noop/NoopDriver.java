package net.openhft.microservices.benchmark.driver.noop;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.microservices.benchmark.driver.api.Driver;
import net.openhft.microservices.benchmark.driver.api.Event;
import net.openhft.microservices.benchmark.driver.api.Producer;

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
