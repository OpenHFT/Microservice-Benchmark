package run.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

public interface GatewayHandler extends ConnectionHeader {
    void run(Marshallable context, Connection connection);
}
