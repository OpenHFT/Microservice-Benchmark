package run.chronicle.queue;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class SimpleHeader extends SelfDescribingMarshallable implements ConnectionHeader {
    private SystemContext systemContext = SystemContext.INSTANCE;

    private String connectionId;

    public SimpleHeader(String connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public SystemContext systemContext() {
        return systemContext;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }
}
