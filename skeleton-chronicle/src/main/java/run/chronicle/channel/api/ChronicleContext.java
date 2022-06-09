package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import net.openhft.chronicle.threads.Pauser;
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
            performClose();
        }
    };

    private String hostname;

    private int port;
    private boolean buffered;

    protected ChronicleContext() {
    }

    public static ChronicleContext newContext() {
        return new ChronicleContext();
    }

    public static ChronicleContext newServerContext(int port) {
        if ((port & 0xFFFF) != port)
            throw new IllegalArgumentException();
        return newContext().hostname(null).port(port);
    }

    public static ChronicleContext newClientContext(String hostname, int port) {
        Objects.requireNonNull(hostname);
        if ((port & 0xFFFF) != port || port == 0)
            throw new IllegalArgumentException();
        return newContext().hostname(hostname).port(port);
    }

    public <I, O> Runnable serviceAsRunnable(ChannelHandler handler, Function<O, I> msFunction, Class<O> tClass) {
        final ChronicleChannelSupplier supplier0 = newChannelSupplier(handler);
        final ChronicleChannel channel0 = supplier0.get();
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

    public ChronicleChannelSupplier newChannelSupplier(ChannelHandler handler) {
        final ChronicleChannelSupplier connectionSupplier = new ChronicleChannelSupplier(this, handler);
        connectionSupplier.hostname(hostname()).port(port()).buffered(buffered()).initiator(true);
        return connectionSupplier;
    }

    @Override
    public void close() {
        closeable.close();
    }

    @Override
    public boolean isClosed() {
        return closeable.isClosed();
    }

    public void addCloseable(Closeable closeable) {
        closeableSet.add(closeable);
    }

    protected void performClose() {
        Closeable.closeQuietly(closeableSet);
        closeableSet.clear();
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

    public boolean buffered() {
        return buffered;
    }

    public ChronicleContext buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }
}
