package run.chronicle.channel;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.Channel;
import run.chronicle.channel.api.ChannelCfg;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.impl.BufferedChannel;
import run.chronicle.channel.impl.SimpleChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ChronicleGatewayMain extends SelfDescribingMarshallable implements Closeable {
    transient volatile boolean closed;
    transient ServerSocketChannel ssc;
    transient Thread thread;
    private int port;
    private boolean buffered;

    public static void main(String[] args) throws IOException {
        ChronicleGatewayMain main = args.length == 0
                ? new ChronicleGatewayMain()
                .port(Integer.getInteger("port", 65432))
                .buffered(Jvm.getBoolean("buffered"))
                : Marshallable.fromFile(ChronicleGatewayMain.class, args[0]);
        System.out.println("Starting  " + main);
        main.run();
    }

    public int port() {
        return port;
    }

    public ChronicleGatewayMain port(int port) {
        this.port = port;
        return this;
    }

    public boolean buffered() {
        return buffered;
    }

    public ChronicleGatewayMain buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }

    public synchronized ChronicleGatewayMain start() throws IOException {
        if (isClosed())
            throw new IllegalStateException("Closed");
        bindSSC();
        if (thread == null) {
            thread = new Thread(this::run, "acceptor");
            thread.setDaemon(true);
            thread.start();
        }
        return this;
    }

    private void bindSSC() throws IOException {
        if (ssc == null) {
            ssc = ServerSocketChannel.open();
            ssc.bind(new InetSocketAddress(port));
        }
    }

    private void run() {
        try {
            bindSSC();
            ChannelCfg channelCfg = new ChannelCfg().port(port);
            ExecutorService service = Executors.newCachedThreadPool(new NamedThreadFactory("connections"));
            while (!isClosed()) {
                final SocketChannel sc = ssc.accept();
                sc.socket().setTcpNoDelay(true);
                final SimpleChannel connection0 = new SimpleChannel(channelCfg, sc);
                Channel channel = buffered ? new BufferedChannel(connection0, Pauser.balanced()) : connection0;
                service.submit(() -> handle(channel));
            }
        } catch (Throwable e) {
            if (!isClosed()) Jvm.error().on(getClass(), e);

        } finally {
            close();
        }
    }

    @Override
    public void close() {
        closed = true;
        Closeable.closeQuietly(ssc);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    void handle(Channel channel) {
        try {
            // get the header
            final Marshallable marshallable = channel.headerIn();
            if (!(marshallable instanceof ChannelHandler)) {
                try (DocumentContext dc = channel.acquireWritingDocument(true)) {
                    dc.wire().write("error").text("The header must be a BrokerHandler");
                }
                channel.close();
                return;
            }
            System.out.println("Server got " + marshallable);
            ChannelHandler bh = (ChannelHandler) marshallable;
            bh.run(this, channel);

        } catch (Throwable t) {
            Jvm.error().on(getClass(), t);
        } finally {
            Closeable.closeQuietly(channel);
        }
    }
}
