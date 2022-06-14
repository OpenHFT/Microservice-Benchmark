package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.AbstractCloseable;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.WireIn;
import run.chronicle.channel.ChronicleGatewayMain;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

public class ChronicleContext extends SelfDescribingMarshallable implements Closeable {
    static {
        addMyPackage(run.chronicle.channel.impl.tcp.Handler.class);
        ;
    }

    private final Set<Closeable> closeableSet =
            Collections.synchronizedSet(
                    Collections.newSetFromMap(
                            new WeakIdentityHashMap<>()));
    private final String url;
    private transient URL _url;
    private transient AbstractCloseable closeable;
    private boolean asServer;
    private boolean buffered;
    private ChronicleGatewayMain gateway;

    protected ChronicleContext(String url) {
        this.url = url;
        init();
    }

    public static ChronicleContext newContext(String url) {
        return new ChronicleContext(url);
    }

    private static void addMyPackage(Class<? extends URLStreamHandler> handlerClass) {
        // Ensure that we are registered as a url protocol handler for JavaFxCss:/path css files.
        String was = System.getProperty("java.protocol.handler.pkgs", "");
        String pkg = handlerClass.getPackage().getName();
        int ind = pkg.lastIndexOf('.');
        assert ind != -1 : "You can't add url handlers in the base package";
        assert "Handler".equals(handlerClass.getSimpleName()) : "A URLStreamHandler must be in a class named Handler; not " + handlerClass.getSimpleName();

        System.setProperty("java.protocol.handler.pkgs",
                pkg.substring(0, ind) + (was.isEmpty() ? "" : "|" + was));
    }

    protected void init() {
        closeable = new AbstractCloseable() {
            @Override
            protected void performClose() throws IllegalStateException {
                performClose();
            }
        };
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
        checkServerRunning();

        final ChronicleChannelSupplier connectionSupplier = new ChronicleChannelSupplier(this, handler);
        final String hostname = url().getHost();
        connectionSupplier
                .protocol(url().getProtocol())
                .hostname(hostname == null || hostname.isEmpty() ? "localhost" : hostname)
                .port(url().getPort())
                .buffered(buffered())
                .initiator(true);
        return connectionSupplier;
    }

    private synchronized void checkServerRunning() {
        if (url().getProtocol().equals("tcp") && "".equals(url().getHost()) && gateway == null) {
            gateway = new ChronicleGatewayMain(url);
            try {
                addCloseable(gateway);
                gateway.start();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
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

    public URL url() {
        if (_url == null) {
            try {
                _url = new URL(url);
            } catch (MalformedURLException e) {
                throw new IORuntimeException(e);
            }
        }
        return _url;
    }

    public boolean asServer() {
        return asServer;
    }

    public ChronicleContext asServer(boolean asServer) {
        this.asServer = asServer;
        return this;
    }

    public boolean buffered() {
        return buffered;
    }

    public ChronicleContext buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }

    @Override
    public void readMarshallable(WireIn wire) throws IORuntimeException {
        super.readMarshallable(wire);
        init();
    }
}
