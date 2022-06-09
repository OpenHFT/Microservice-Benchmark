package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.MarshallableIn;
import net.openhft.chronicle.wire.MarshallableOut;
import run.chronicle.channel.impl.BufferedChannel;
import run.chronicle.channel.impl.SimpleChannel;

public interface Channel extends Closeable, MarshallableOut, MarshallableIn {
    static Channel createFor(ChannelCfg session, ChannelHeader headerOut) {
        SimpleChannel simpleConnection = new SimpleChannel(session, headerOut);
        final ChannelHeader marshallable = simpleConnection.headerIn();
        System.out.println("Client got " + marshallable);
        Channel channel = session.buffered()
                ? new BufferedChannel(simpleConnection, session.pauser().get())
                : simpleConnection;
        return channel;
    }

    ChannelCfg channelCfg();

    Marshallable headerOut();

    Marshallable headerIn();

    default Runnable subscriberAsRunnable(Object subscriptionHandler) {
        final MethodReader echoingReader = methodReader(subscriptionHandler);
        return () -> {
            try {
                Pauser pauser = channelCfg().pauser().get();
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
