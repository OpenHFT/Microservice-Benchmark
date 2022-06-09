package run.chronicle.channel.impl;

import net.openhft.affinity.AffinityThreadFactory;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import run.chronicle.channel.api.ChannelHeader;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;
import run.chronicle.channel.api.EventPoller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class BufferedChronicleChannel implements ChronicleChannel, Closeable {
    private final SimpleChronicleChannel connection;
    private final Pauser pauser;
    private final WireExchanger exchanger = new WireExchanger();
    private final ExecutorService bgWriter;
    private final int lingerNs;
    private volatile EventPoller eventPoller;

    public BufferedChronicleChannel(SimpleChronicleChannel connection, Pauser pauser) {
        this(connection, pauser, 8);
    }

    public BufferedChronicleChannel(SimpleChronicleChannel connection, Pauser pauser, int lingerUs) {
        this.connection = connection;
        this.pauser = pauser;
        String desc = connection.connectionCfg().initiator() ? "init" : "accp";
        final String writer = connection.headerIn().connectionId() + "~" + desc + "~" + "writer";
        final ThreadFactory factory = isBusy(pauser)
                ? new AffinityThreadFactory(writer, true)
                : new NamedThreadFactory(writer, true);
        bgWriter = Executors.newSingleThreadExecutor(factory);
        bgWriter.submit(this::bgWrite);
        lingerNs = lingerUs * 1000;
    }

    public EventPoller eventPoller() {
        return eventPoller;
    }

    public BufferedChronicleChannel eventPoller(EventPoller eventPoller) {
        this.eventPoller = eventPoller;
        return this;
    }

    // TODO Need a better test
    private boolean isBusy(Pauser pauser) {
        pauser.pause();
        return pauser.countPaused() == 0;
    }

    private void bgWrite() {
        try {
            while (!isClosing()) {
                long start = System.nanoTime();
                connection.checkConnected();
                final Wire wire = exchanger.acquireConsumer();
                if (wire.bytes().isEmpty()) {
                    final EventPoller eventPoller = this.eventPoller;
                    if (eventPoller == null || !eventPoller.onPoll(this)) {
                        pauser.pause();
                    }
                    continue;
                }
                appendBatchEnd(wire);
                pauser.reset();
//                long size = wire.bytes().readRemaining();
                connection.flushOut(wire);

                while (System.nanoTime() < start + lingerNs) {
                    pauser.pause();
                }
            }
        } catch (Throwable t) {
            if (!isClosing())
                Jvm.warn().on(getClass(), "bgWriter died", t);
        } finally {
            bgWriter.shutdown();
        }
    }

    private void appendBatchEnd(Wire wire) {
        try (final DocumentContext dc = wire.writingDocument(true)) {
            dc.wire().write("be").nu11();
        }
    }

    @Override
    public ChronicleChannelCfg channelCfg() {
        return connection.channelCfg();
    }

    @Override
    public DocumentContext readingDocument() {
        return connection.readingDocument();
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return exchanger.writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return exchanger.acquireWritingDocument(metaData);
    }

    @Override
    public ChannelHeader headerOut() {
        return connection.headerOut();
    }

    @Override
    public ChannelHeader headerIn() {
        return connection.headerIn();
    }

    @Override
    public boolean isClosing() {
        return connection.isClosing();
    }

    @Override
    public void close() {
        connection.close();
    }

    @Override
    public boolean isClosed() {
        return connection.isClosed();
    }
}
