package run.chronicle.channel.api;

import java.util.function.Supplier;

public class ChronicleChannelSupplier extends ChronicleChannelCfg implements Supplier<ChronicleChannel> {
    private final ChronicleContext context;
    private final ChannelHandler handler;

    public ChronicleChannelSupplier(ChronicleContext context, ChannelHandler handler) {
        this.context = context;
        this.handler = handler;
    }

    @Override
    public ChronicleChannel get() {
        final ChronicleChannel channel = ChronicleChannel.newChannel(this, handler);
        context.addCloseable(channel);
        return channel;
    }
}
