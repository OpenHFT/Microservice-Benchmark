package run.chronicle.queue;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.MarshallableOut;
import run.chronicle.queue.impl.BufferedConnection;
import run.chronicle.queue.impl.SimpleConnection;

public interface Connection extends Closeable, MarshallableOut, MarshallableIn {
    static Connection createFor(SessionCfg session) {
        SimpleConnection simpleConnection = new SimpleConnection(session);
        Connection connection = session.buffered()
                ? new BufferedConnection(simpleConnection, session.pauser().get())
                : simpleConnection;
        connection.methodWriter(true, ExpectsHeader.class)
                .header(new SimpleHeader());
        Marshallable[] header = new Marshallable[1];
        final ExpectsHeader expectsHeader = marshallable -> header[0] = marshallable;
        connection.methodReaderBuilder()
                .metaDataHandler(expectsHeader)
                .build(IgnoresEverything.INSTANCE)
                .readOne();
        if (header[0] == null)
            throw new IORuntimeException("No header returned");
        System.out.println("Client got " + header[0]);
        return connection;
    }

    SessionCfg sessionCfg();
}
