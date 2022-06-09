package run.chronicle.channel.impl;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.io.SimpleCloseable;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.*;
import run.chronicle.channel.SimpleHandler;
import run.chronicle.channel.api.Channel;
import run.chronicle.channel.api.ChannelCfg;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.api.ChannelHeader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.SocketChannel;
import java.util.Objects;

import static net.openhft.chronicle.core.io.Closeable.closeQuietly;

public class SimpleChannel extends SimpleCloseable implements Channel {
    public static final String HEADER = "header";
    private static final ChannelHeader NO_HEADER = Mocker.ignored(ChannelHeader.class);
    private final ChannelCfg channelCfg;
    private SocketChannel sc;
    private Wire in = createBuffer(), out = createBuffer();
    private DocumentContextHolder dch = new ConnectionDocumentContextHolder();
    private ChannelHeader headerIn;
    private ChannelHeader headerOut;

    public SimpleChannel(ChannelCfg channelCfg, ChannelHeader headerOut) {
        this.channelCfg = Objects.requireNonNull(channelCfg);
        this.headerOut = Objects.requireNonNull(headerOut);

        this.sc = null;
        assert channelCfg.initiator();
        checkConnected();
    }

    public SimpleChannel(ChannelCfg channelCfg, SocketChannel sc) {
        this.channelCfg = Objects.requireNonNull(channelCfg);
        this.sc = Objects.requireNonNull(sc);

        this.headerOut = null;
        assert !channelCfg.initiator();
    }

    @Override
    public ChannelCfg channelCfg() {
        return channelCfg;
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

        } catch (AsynchronousCloseException e) {
            close();
            throw new ClosedIORuntimeException(e.toString());

        } catch (IOException e) {
            if ("An existing connection was forcibly closed by the remote host".equals(e.getMessage()))
                throw new ClosedIORuntimeException("Closed");
            throw new IORuntimeException(e);
        }
        if (read < 0) {
            close();
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
        if (channelCfg.initiator()) {
            long end = System.nanoTime()
                    + (long) (channelCfg.connectionTimeoutSecs() * 1e9);
            for (int delay = 1; ; delay++) {
                try {
                    sc = SocketChannel.open(channelCfg.remote());
                    if (channelCfg.pauser() == PauserMode.busy)
                        sc.configureBlocking(false);
                    sc.socket().setTcpNoDelay(true);
                    writeHeader();
                    readHeader();
                    break;

                } catch (IOException e) {
                    if (System.nanoTime() * 0 > end)
                        throw new IORuntimeException(e);
                    Jvm.pause(delay);
                }
            }
        }
        in.clear();
        out.clear();
    }

    @Override
    protected void performClose() {
        super.performClose();
        Closeable.closeQuietly(sc);
    }

    synchronized void acceptorRespondToHeader() {
        headerOut = NO_HEADER;
        readHeader();
        headerOut = headerIn instanceof ChannelHandler
                ? ((ChannelHandler) headerIn).responseHeader()
                : new SimpleHandler(headerIn.connectionId());
        writeHeader();
    }

    private void writeHeader() {
        try (DocumentContext dc = writingDocument(true)) {
            dc.wire().write(HEADER).object(headerOut);
        }
    }

    @Override
    public ChannelHeader headerOut() {
        if (headerOut == null)
            acceptorRespondToHeader();
        return headerOut;
    }

    @Override
    public ChannelHeader headerIn() {
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
                headerIn = dc.wire().getValueIn().object(ChannelHeader.class);
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

    public ChannelCfg connectionCfg() {
        return channelCfg;
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
