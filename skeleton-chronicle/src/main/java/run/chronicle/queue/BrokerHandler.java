package run.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

public interface BrokerHandler {
    void run(Marshallable context, Connection connection);
}
