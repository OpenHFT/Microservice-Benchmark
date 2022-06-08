package run.chronicle.channel.api;

import java.util.function.Supplier;

public class ChannelSupplier extends ChannelCfg implements Supplier<Channel> {
    private final ChronicleContext context;
    private final ChannelHandler handler;

    public ChannelSupplier(ChronicleContext context, ChannelHandler handler) {
        this.context = context;
        this.handler = handler;
    }

    @Override
    public Channel get() {
        final Channel channel = Channel.createFor(this, handler);
        context.addCloseable(channel);
        return channel;
    }
}
