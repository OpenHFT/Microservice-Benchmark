package run.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.queue.impl.BufferedConnection;
import run.chronicle.queue.impl.ClosedIORuntimeException;
import run.chronicle.queue.impl.SimpleConnection;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChronicleServerMain extends SelfDescribingMarshallable implements Closeable {
    int port;
    Marshallable microservice;
    boolean buffered;
    transient ServerSocketChannel ssc;
    transient volatile boolean closed;

    public static void main(String[] args) throws IOException {
        ChronicleServerMain main = Marshallable.fromFile(ChronicleServerMain.class, args[0]);
        main.run();
    }

    void run() {
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
                service.submit(() -> new ConnectionHandler(connection).run());
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

    class ConnectionHandler {
        final Connection connection;

        public ConnectionHandler(Connection connection) {
            this.connection = connection;
        }

        void run() {
            try {
                System.out.println("Server got " + connection.headerIn());

                final Marshallable microservice = ChronicleServerMain.this.microservice.deepCopy();
                final MethodReader reader = connection.methodReaderBuilder().build(microservice);
                final Field field = Jvm.getFieldOrNull(microservice.getClass(), "out");
                if (field == null)
                    throw new IllegalStateException("Microservice " + microservice + " must have a field called out");
                Object out = connection.methodWriter(field.getType());
                try (AffinityLock lock = AffinityLock.acquireCore()) {
                    field.set(microservice, out);
                    while (!((Closeable) microservice).isClosed()) {
                        reader.readOne();
                    }
                } catch (ClosedIORuntimeException e) {
                    Thread.yield();
                    if (!((Closeable) microservice).isClosed())
                        Jvm.debug().on(getClass(), "readOne threw " + e);

                } catch (Exception e) {
                    Thread.yield();
                    if (!((Closeable) microservice).isClosed())
                        Jvm.warn().on(getClass(), "readOne threw ", e);
                }
            } catch (Throwable t) {
                Jvm.error().on(getClass(), t);
            } finally {
                Closeable.closeQuietly(connection);
            }
        }
    }
}
