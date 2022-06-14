package run.chronicle.channel.api;

import run.chronicle.channel.SimpleHandler;

public interface ChannelHandler extends ChannelHeader {

    default ChannelHeader responseHeader() {
        return new SimpleHandler(connectionId());
    }

    void run(ChronicleContext context, ChronicleChannel channel);

    default ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        throw new UnsupportedOperationException("Internal protocol not supported");
    }
}
