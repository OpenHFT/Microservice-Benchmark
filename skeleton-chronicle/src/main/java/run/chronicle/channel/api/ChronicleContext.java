package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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

    private ChronicleContext() {
    }

    public static ChronicleContext create() {
        return new ChronicleContext();
    }

    public static ChronicleContext asTester() {
        return create().hostname(null).port(0);
    }

    public static ChronicleContext asServer(int port) {
        if ((port & 0xFFFF) != port)
            throw new IllegalArgumentException();
        return create().hostname(null).port(port);
    }

    public static ChronicleContext asClient(String hostname, int port) {
        Objects.requireNonNull(hostname);
        if ((port & 0xFFFF) != port || port == 0)
            throw new IllegalArgumentException();
        return create().hostname(hostname).port(port);
    }

    public <I, O> Runnable serviceAsRunnable(ChannelHandler handler, Function<O, I> msFunction, Class<O> tClass) {
        final ChannelSupplier supplier0 = newChannelSupplier(handler);
        final Channel channel0 = supplier0.get();
        I microservice = msFunction.apply(channel0.methodWriter(tClass));
        final MethodReader echoingReader = channel0.methodReader(microservice);
        return () -> {
            try {
                Pauser pauser = Pauser.balanced();
                while (!channel0.isClosed()) {
                    if (echoingReader.readOne())
                        pauser.reset();
                    else
                        pauser.pause();
                }
            } catch (Throwable t) {
                Jvm.warn().on(ChronicleContext.class, "Error stopped reading thread", t);
            }
        };
    }

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
