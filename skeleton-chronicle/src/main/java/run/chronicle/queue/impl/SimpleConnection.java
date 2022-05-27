package run.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.*;
import run.chronicle.queue.Connection;
import run.chronicle.queue.ConnectionCfg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Objects;
import java.util.function.Function;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class SimpleConnection extends SimpleCloseable implements Connection {
    public static final String HEADER = "header";
    private static final Marshallable NO_MARSHALLABLE = new Marshallable() {
    };
    private final ConnectionCfg connectionCfg;
    private final Function<Marshallable, Marshallable> responseHeaderFunction;
    private Marshallable headerOut;
    private SocketChannel sc;
    private Wire in = createBuffer(), out = createBuffer();
    private DocumentContextHolder dch = new ConnectionDocumentContextHolder();
    private Marshallable headerIn;

    public SimpleConnection(ConnectionCfg connectionCfg, Marshallable headerOut) {
        this.connectionCfg = Objects.requireNonNull(connectionCfg);
        this.headerOut = Objects.requireNonNull(headerOut);

        this.responseHeaderFunction = null;
        this.sc = null;
        assert connectionCfg.initiator();
        checkConnected();
    }

    public SimpleConnection(ConnectionCfg connectionCfg, SocketChannel sc, Function<Marshallable, Marshallable> responseHeaderFunction) {
        this.connectionCfg = Objects.requireNonNull(connectionCfg);
        this.responseHeaderFunction = Objects.requireNonNull(responseHeaderFunction);
        this.sc = Objects.requireNonNull(sc);

        this.headerOut = null;
        assert !connectionCfg.initiator();
    }

    @Override
    public ConnectionCfg sessionCfg() {
        return connectionCfg;
    }

    private void flush() {
        flushOut(out);
    }

    void flushOut(Wire out) {
        final Bytes<ByteBuffer> bytes = (Bytes) out.bytes();
        if (out.bytes().writeRemaining() <= 0)
            return;
        ByteBuffer bb = bytes.underlyingObject();
        bb.position(Math.toIntExact(bytes.readPosition()));
        bb.limit(Math.toIntExact(bytes.readLimit()));
        while (bb.remaining() > 0) {
            int len;
            try {
                len = sc.write(bb);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
            if (len < 0)
                throw new ClosedIORuntimeException("Closed");
        }
        out.clear();
    }

    private Wire createBuffer() {
        final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer(64 << 10);
        return WireType.BINARY_LIGHT.apply(bytes);
    }

    @Override
    public DocumentContext readingDocument() {
        checkConnected();
        final Bytes<ByteBuffer> bytes = (Bytes) in.bytes();
        if (bytes.readRemaining() == 0)
            bytes.clear();
        final DocumentContext dc = in.readingDocument();
        if (dc.isPresent())
            return dc;
        if (bytes.readPosition() > (32 << 10))
            bytes.compact();
        final ByteBuffer bb = bytes.underlyingObject();
        bb.position(Math.toIntExact(bytes.writePosition()));
        bb.limit(Math.min(bb.capacity(), Math.toIntExact(bytes.writeLimit())));
        int read;
        try {
            read = sc.read(bb);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        if (read < 0) {
            throw new ClosedIORuntimeException("Closed");
        }
        bytes.writeSkip(read);
        return in.readingDocument();
    }

    synchronized void checkConnected() {
        if (sc != null && sc.isOpen()) {
            if (headerOut == null) {
                acceptorRespondToHeader();
            }
            return;
        }
        closeQuietly(sc);
        if (isClosing())
            throw new IllegalStateException("Closed");
        if (connectionCfg.initiator()) {
            try {
                sc = SocketChannel.open(connectionCfg.remote());
                if (connectionCfg.pauser() == PauserMode.busy)
                    sc.configureBlocking(false);
                sc.socket().setTcpNoDelay(true);
                writeHeader();
                readHeader();
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
        in.clear();
        out.clear();
    }

    synchronized void acceptorRespondToHeader() {
        headerOut = NO_MARSHALLABLE;
        readHeader();
        headerOut = responseHeaderFunction.apply(headerIn);
        writeHeader();
    }

    private void writeHeader() {
        try (DocumentContext dc = writingDocument(true)) {
            dc.wire().write(HEADER).object(headerOut);
        }
    }

    @Override
    public Marshallable headerOut() {
        if (headerOut == null)
            acceptorRespondToHeader();
        return headerOut;
    }

    @Override
    public Marshallable headerIn() {
        if (headerIn == null)
            acceptorRespondToHeader();
        return headerIn;
    }

    private void readHeader() {
        while (!Thread.currentThread().isInterrupted()) {
            try (DocumentContext dc = readingDocument()) {
                if (!dc.isPresent()) {
                    Thread.yield();
                    continue;
                }
                final String s = dc.wire().readEvent(String.class);
                if (!HEADER.equals(s)) {
                    Jvm.warn().on(getClass(), "Unexpected first message type " + s);
                }
                headerIn = dc.wire().getValueIn().object(Marshallable.class);
                break;
            }
        }
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        checkConnected();
        final DocumentContext dc = out.writingDocument(metaData);
        dch.documentContext(dc);
        return dch;
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        checkConnected();
        final DocumentContext dc = out.acquireWritingDocument(metaData);
        dch.documentContext(dc);
        return dch;
    }

    private class ConnectionDocumentContextHolder extends DocumentContextHolder implements WriteDocumentContext {
        private boolean chainedElement;

        @Override
        public void close() {
            super.close();
            if (!chainedElement)
                flush();
        }

        @Override
        public void start(boolean metaData) {

        }

        @Override
        public boolean chainedElement() {
            return chainedElement;
        }

        @Override
        public void chainedElement(boolean chainedElement) {
            this.chainedElement = chainedElement;
            final DocumentContext dc = documentContext();
            if (dc instanceof WriteDocumentContext)
                ((WriteDocumentContext) dc).chainedElement(chainedElement);
        }
    }
}
