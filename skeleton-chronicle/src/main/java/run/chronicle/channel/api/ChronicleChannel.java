package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.MarshallableOut;
import run.chronicle.channel.impl.BufferedChronicleChannel;
import run.chronicle.channel.impl.ClosedIORuntimeException;
import run.chronicle.channel.impl.TCPChronicleChannel;

public interface ChronicleChannel extends Closeable, MarshallableOut, MarshallableIn {
    static ChronicleChannel newChannel(ChronicleChannelCfg session, ChannelHeader headerOut) {
        if (session.port() <= 0)
            throw new UnsupportedOperationException("local service not supported");
        TCPChronicleChannel simpleConnection = new TCPChronicleChannel(session, headerOut);
        final ChannelHeader marshallable = simpleConnection.headerIn();
        System.out.println("Client got " + marshallable);
        ChronicleChannel channel = session.buffered()
                ? new BufferedChronicleChannel(simpleConnection, session.pauserMode().get())
                : simpleConnection;
        return channel;
    }

    ChronicleChannelCfg channelCfg();

    ChannelHeader headerOut();

    ChannelHeader headerIn();

    /**
     * Read one event and return a value
     *
     * @param eventType of the event read
     * @return any data transfer object
     * @throws ClosedIORuntimeException
     */
    default <T> T readOne(StringBuilder eventType, Class<T> expectedType) throws ClosedIORuntimeException {
        while (!isClosed()) {
            try (DocumentContext dc = readingDocument()) {
                if (dc.isPresent()) {
                    return (T) dc.wire().read(eventType).object(expectedType);
                }
            }
        }
        throw new ClosedIORuntimeException("Closed");
    }

    /**
     * Reading all events and call the same method on the subscription handler
     *
     * @param subscriptionHandler to handle events
     * @return a Runnable that can be passed to a Thread or ExecutorService
     */
    default Runnable subscriberAsRunnable(Object subscriptionHandler) {
        final MethodReader echoingReader = methodReader(subscriptionHandler);
        return () -> {
            try {
                Pauser pauser = channelCfg().pauserMode().get();
                while (!isClosed()) {
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
}
