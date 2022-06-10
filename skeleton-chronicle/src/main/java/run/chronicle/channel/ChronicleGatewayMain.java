package run.chronicle.channel;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;
import run.chronicle.channel.api.ChronicleContext;
import run.chronicle.channel.impl.BufferedChronicleChannel;
import run.chronicle.channel.impl.TCPChronicleChannel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ChronicleGatewayMain extends ChronicleContext implements Closeable {
    transient ServerSocketChannel ssc;
    transient Thread thread;
    private boolean buffered;
    private PauserMode pauserMode;

    public static void main(String... args) throws IOException {
        ChronicleGatewayMain main = args.length == 0
                ? new ChronicleGatewayMain()
                .port(Integer.getInteger("port", 65432))
                .buffered(Jvm.getBoolean("buffered"))
                .pauserMode(PauserMode.valueOf(System.getProperty("pauserMode", PauserMode.balanced.name())))
                : Marshallable.fromFile(ChronicleGatewayMain.class, args[0]);
        System.out.println("Starting  " + main);
        main.run();
    }

    public ChronicleGatewayMain port(int port) {
        super.port(port);
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
            ssc.socket().setReuseAddress(true);
            ssc.bind(new InetSocketAddress(port()));
        }
    }

    private void run() {
        try {
            bindSSC();
            ChronicleChannelCfg channelCfg = new ChronicleChannelCfg().port(port()).pauserMode(pauserMode).buffered(buffered);
            ExecutorService service = Executors.newCachedThreadPool(new NamedThreadFactory("connections"));
            while (!isClosed()) {
                final SocketChannel sc = ssc.accept();
                sc.socket().setTcpNoDelay(true);
                final TCPChronicleChannel connection0 = new TCPChronicleChannel(channelCfg, sc);
                ChronicleChannel channel = buffered ? new BufferedChronicleChannel(connection0, Pauser.balanced()) : connection0;
                service.submit(() -> handle(channel));
            }
        } catch (Throwable e) {
            if (!isClosed()) Jvm.error().on(getClass(), e);

        } finally {
            close();
        }
    }

    @Override
    protected void performClose() {
        super.performClose();
        Closeable.closeQuietly(ssc);
    }

    void handle(ChronicleChannel channel) {
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

    public ChronicleGatewayMain pauserMode(PauserMode pauserMode) {
        this.pauserMode = pauserMode;
        return this;
    }
}
