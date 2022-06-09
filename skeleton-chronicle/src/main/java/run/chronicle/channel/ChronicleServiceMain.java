package run.chronicle.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;
import run.chronicle.channel.impl.BufferedChronicleChannel;
import run.chronicle.channel.impl.ClosedIORuntimeException;
import run.chronicle.channel.impl.SimpleChronicleChannel;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChronicleServiceMain extends SelfDescribingMarshallable implements Closeable {
    int port;
    Marshallable microservice;
    boolean buffered;
    transient ServerSocketChannel ssc;
    transient volatile boolean closed;

    public static void main(String[] args) throws IOException {
        ChronicleServiceMain main = Marshallable.fromFile(ChronicleServiceMain.class, args[0]);
        main.run();
    }

    void run() {
        Thread.currentThread().setName("acceptor");
        try {
            ssc = ServerSocketChannel.open();
            ssc.bind(new InetSocketAddress(port));
            ChronicleChannelCfg channelCfg = new ChronicleChannelCfg().port(port);
            ExecutorService service = Executors.newCachedThreadPool(new NamedThreadFactory("connections"));
            while (!isClosed()) {
                final SocketChannel sc = ssc.accept();
                sc.socket().setTcpNoDelay(true);
                final SimpleChronicleChannel connection0 = new SimpleChronicleChannel(channelCfg, sc);
                ChronicleChannel channel = buffered ? new BufferedChronicleChannel(connection0, Pauser.balanced()) : connection0;
                service.submit(() -> new ConnectionHandler(channel).run());
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
        final ChronicleChannel channel;

        public ConnectionHandler(ChronicleChannel channel) {
            this.channel = channel;
        }

        void run() {
            try {
                System.out.println("Server got " + channel.headerIn());

                final Marshallable microservice = ChronicleServiceMain.this.microservice.deepCopy();
                final MethodReader reader = channel.methodReaderBuilder().build(microservice);
                final Field field = Jvm.getFieldOrNull(microservice.getClass(), "out");
                if (field == null)
                    throw new IllegalStateException("Microservice " + microservice + " must have a field called out");
                Object out = channel.methodWriter(field.getType());
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
                Closeable.closeQuietly(channel);
            }
        }
    }
}
