package run.chronicle.channel.api;

import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.util.Collections;
import java.util.Set;

public class ChronicleContext extends SelfDescribingMarshallable implements Closeable {
    private final Set<Closeable> closeableSet =
            Collections.synchronizedSet(
                    Collections.newSetFromMap(
                            new WeakIdentityHashMap<>()));
    private AbstractCloseable closeable = new AbstractCloseable() {
        @Override
        protected void performClose() throws IllegalStateException {
            Closeable.closeQuietly(closeableSet);
            closeableSet.clear();
        }
    };
    private String hostname;
    private int port;

    public String hostname() {
        return hostname;
    }

    public ChronicleContext hostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int port() {
        return port;
    }

    public ChronicleContext port(int port) {
        this.port = port;
        return this;
    }

    @Override
    public void close() {
        closeable.close();
    }

    @Override
    public boolean isClosed() {
        return closeable.isClosed();
    }

    public ChannelSupplier newChannelSupplier(ChannelHandler handler) {
        final ChannelSupplier connectionSupplier = new ChannelSupplier(this, handler);
        connectionSupplier.hostname(hostname()).port(port).initiator(true);
        return connectionSupplier;
    }

    public void addCloseable(Closeable closeable) {
        closeableSet.add(closeable);
    }
}
