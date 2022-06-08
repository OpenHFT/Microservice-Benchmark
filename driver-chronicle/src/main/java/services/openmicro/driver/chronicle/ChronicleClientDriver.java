package services.openmicro.driver.chronicle;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.Channel;
import run.chronicle.channel.api.ChannelCfg;
import services.openmicro.driver.api.Driver;
import services.openmicro.driver.api.Event;
import services.openmicro.driver.api.Producer;

import java.util.function.Consumer;

import static net.openhft.chronicle.wire.WireType.JSON;

@UsedViaReflection
public class ChronicleClientDriver extends SelfDescribingMarshallable implements Driver {
    ChannelCfg session;
    ChronicleEvent event;

    transient MethodReader reader1;
    transient ChronicleEventHandler eventHandler1, eventHandler2;
    volatile boolean running = true;
    private EventMicroservice service;

    @Override
    public void init() {
        Channel channel = Channel.createFor(session, null /* TODO */);

        service = new EventMicroservice(eventHandler2);

        reader1 = channel.methodReader(service);
        System.out.println("Event size in JSON: " + JSON.asString(event));
    }

    @Override
    public void start() {
    }

    @Override
    public Producer createProducer(Consumer<Event> eventConsumer) {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        Driver.super.close();
        running = false;
    }
}
