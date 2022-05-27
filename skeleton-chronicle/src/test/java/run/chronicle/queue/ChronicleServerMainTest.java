package run.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.junit.Test;

public class ChronicleServerMainTest {

    @Test
    public void handshake() {
        String cfg = "" +
                "port: 65432\n" +
                "microservice: !run.chronicle.queue.ClosingMicroservice { }";
        ChronicleServerMain main = Marshallable.fromString(ChronicleServerMain.class, cfg);
        Thread t = new Thread(main::run);
        t.setDaemon(true);
        t.start();

        final ConnectionCfg connectionCfg = new ConnectionCfg().hostname("localhost").port(65432).initiator(true).buffered(true);
        Connection client = Connection.createFor(connectionCfg, new SimpleHeader());
        client.close();
    }
}

interface NoOut {
    Closeable out();
}

class ClosingMicroservice extends SelfDescribingMarshallable implements Closeable {
    NoOut out;

    @Override
    public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isClosed() {
        return true;
    }
}
