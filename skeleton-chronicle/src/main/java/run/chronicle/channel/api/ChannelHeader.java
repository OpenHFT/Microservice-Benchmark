package run.chronicle.channel.api;

import net.openhft.chronicle.wire.Marshallable;

public interface ChannelHeader extends Marshallable {
    SystemContext systemContext();

    String connectionId();
}
