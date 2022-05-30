package run.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.queue.impl.BufferedConnection;
import run.chronicle.queue.impl.SimpleConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ChronicleBrokerMain extends SelfDescribingMarshallable implements Runnable, Closeable {
    transient ServerSocketChannel ssc;
    transient volatile boolean closed;
    private int port;
    private boolean buffered;

    public static void main(String[] args) throws IOException {
        ChronicleBrokerMain main = Marshallable.fromFile(ChronicleBrokerMain.class, args[0]);
        main.run();
    }

    public int port() {
        return port;
    }

    public ChronicleBrokerMain port(int port) {
        this.port = port;
        return this;
    }

    public boolean buffered() {
        return buffered;
    }

    public ChronicleBrokerMain buffered(boolean buffered) {
        this.buffered = buffered;
        return this;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("acceptor");
        try {
            ssc = ServerSocketChannel.open();
            ssc.bind(new InetSocketAddress(port));
            ConnectionCfg connectionCfg = new ConnectionCfg().port(port);
            ExecutorService service = Executors.newCachedThreadPool(new NamedThreadFactory("connections"));
            while (!isClosed()) {
                final SocketChannel sc = ssc.accept();
                sc.socket().setTcpNoDelay(true);
                final SimpleConnection connection0 = new SimpleConnection(connectionCfg, sc, h -> h);
                Connection connection = buffered ? new BufferedConnection(connection0, Pauser.balanced()) : connection0;
                service.submit(() -> handle(connection));
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

    void handle(Connection connection) {
        try {
            // get the header
            final Marshallable marshallable = connection.headerIn();
            if (!(marshallable instanceof BrokerHandler)) {
                try (DocumentContext dc = connection.acquireWritingDocument(true)) {
                    dc.wire().write("error").text("The header must be a BrokerHandler");
                }
                connection.close();
                return;
            }
            System.out.println("Server got " + marshallable);
            BrokerHandler bh = (BrokerHandler) marshallable;
            bh.run(this, connection);

        } catch (Throwable t) {
            Jvm.error().on(getClass(), t);
        } finally {
            Closeable.closeQuietly(connection);
        }
    }
}
