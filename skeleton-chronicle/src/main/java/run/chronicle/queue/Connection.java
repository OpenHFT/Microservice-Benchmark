package run.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.MarshallableOut;
import run.chronicle.queue.impl.BufferedConnection;
import run.chronicle.queue.impl.SimpleConnection;

public interface Connection extends Closeable, MarshallableOut, MarshallableIn {
    static Connection createFor(ConnectionCfg session, Marshallable headerOut) {
        SimpleConnection simpleConnection = new SimpleConnection(session, headerOut);
        Connection connection = session.buffered()
                ? new BufferedConnection(simpleConnection, session.pauser().get())
                : simpleConnection;
        System.out.println("Client got " + connection.headerIn());
        return connection;
    }

    ConnectionCfg sessionCfg();

    Marshallable headerOut();

    Marshallable headerIn();
}
