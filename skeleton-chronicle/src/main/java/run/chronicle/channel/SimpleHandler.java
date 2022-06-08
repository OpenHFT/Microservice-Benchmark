package run.chronicle.channel;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.ChannelHeader;
import run.chronicle.channel.api.SystemContext;

public class SimpleHandler extends SelfDescribingMarshallable implements ChannelHeader {
    private SystemContext systemContext = SystemContext.INSTANCE;

    private String connectionId;

    public SimpleHandler(String connectionId) {
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
