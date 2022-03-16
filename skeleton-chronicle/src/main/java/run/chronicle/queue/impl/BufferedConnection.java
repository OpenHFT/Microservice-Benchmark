package run.chronicle.queue.impl;

import net.openhft.affinity.AffinityThreadFactory;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.threads.NamedThreadFactory;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import net.openhft.chronicle.wire.Wire;
import run.chronicle.queue.Connection;
import run.chronicle.queue.SessionCfg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class BufferedConnection extends SimpleCloseable implements Connection {
    private final SimpleConnection connection;
    private final Pauser pauser;
    private final WireExchanger exchanger = new WireExchanger();
    private final ExecutorService bgWriter;
    private final int lingerNs;

    public BufferedConnection(SimpleConnection connection, Pauser pauser) {
        this(connection, pauser, 10);
    }

    public BufferedConnection(SimpleConnection connection, Pauser pauser, int lingerUs) {
        this.connection = connection;
        this.pauser = pauser;
        final ThreadFactory factory = isBusy(pauser)
                ? new AffinityThreadFactory("writer", true)
                : new NamedThreadFactory("writer", true);
        bgWriter = Executors.newSingleThreadExecutor(factory);
        bgWriter.submit(this::bgWrite);
        lingerNs = lingerUs * 1000;
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
                    pauser.pause();
                    continue;
                }
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

    @Override
    public SessionCfg sessionCfg() {
        return connection.sessionCfg();
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
    protected void performClose() {
        super.performClose();

        Closeable.closeQuietly(connection);
    }
}
