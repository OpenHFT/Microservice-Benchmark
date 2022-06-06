package run.chronicle.queue;

import net.openhft.chronicle.wire.Marshallable;

public interface ConnectionHeader extends Marshallable {
    SystemContext systemContext();

    default ConnectionHeader responseHeader() {
        return new SimpleHeader();
    }
}
