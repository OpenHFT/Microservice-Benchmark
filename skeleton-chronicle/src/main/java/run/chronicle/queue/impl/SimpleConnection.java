package run.chronicle.queue.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.wire.*;
import run.chronicle.queue.Connection;
import run.chronicle.queue.SessionCfg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class SimpleConnection extends SimpleCloseable implements Connection {
    SessionCfg sessionCfg;
    SocketChannel sc;
    Wire in = createBuffer(), out = createBuffer();
    DocumentContextHolder dch = new ConnectionDocumentContextHolder();

    public SimpleConnection(SessionCfg sessionCfg) {
        this.sessionCfg = sessionCfg;
        assert sessionCfg.initiator();
    }

    public SimpleConnection(SessionCfg sessionCfg, SocketChannel sc) {
        this.sessionCfg = sessionCfg;
        this.sc = sc;
    }

    @Override
    public SessionCfg sessionCfg() {
        return sessionCfg;
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
            int len = 0;
            try {
                len = sc.write(bb);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
            if (len < 0)
                throw new IORuntimeException("Closed");
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
        int read = 0;
        try {
            read = sc.read(bb);
        } catch (IOException e) {
            throw new IORuntimeException(e);
        }
        if (read < 0) {
            throw new IORuntimeException("Closed");
        }
        bytes.writeSkip(read);
        return in.readingDocument();
    }

    synchronized void checkConnected() {
        if (sc != null && sc.isOpen()) {
            return;
        }
        closeQuietly(sc);
        if (isClosing())
            throw new IllegalStateException("Closed");
        if (sessionCfg.initiator()) {
            try {
                sc = SocketChannel.open(sessionCfg.remote());
                sc.socket().setTcpNoDelay(true);
            } catch (IOException e) {
                throw new IORuntimeException(e);
            }
        }
        in.clear();
        out.clear();
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
