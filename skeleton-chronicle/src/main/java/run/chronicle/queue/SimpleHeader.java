package run.chronicle.queue;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class SimpleHeader extends SelfDescribingMarshallable implements ConnectionHeader {
    private SystemContext systemContext = SystemContext.INSTANCE;

    @Override
    public SystemContext systemContext() {
        return systemContext;
    }
}
