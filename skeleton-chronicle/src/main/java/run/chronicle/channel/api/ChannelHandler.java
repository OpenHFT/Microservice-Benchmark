package run.chronicle.channel.api;

import run.chronicle.channel.OkHeader;

public interface ChannelHandler extends ChannelHeader {

    default ChannelHeader responseHeader() {
        return new OkHeader(connectionId());
    }

    void run(ChronicleContext context, ChronicleChannel channel);

    default ChronicleChannel asInternalChannel(ChronicleContext context, ChronicleChannelCfg channelCfg) {
        throw new UnsupportedOperationException("Internal protocol not supported");
    }
}
