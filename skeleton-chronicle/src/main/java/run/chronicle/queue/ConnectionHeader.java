package run.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

public interface ConnectionHeader extends Marshallable {
    SystemContext systemContext();

    String connectionId();

    default ConnectionHeader responseHeader() {
        return new SimpleHeader(connectionId());
    }
}
