package run.chronicle.channel.impl;

import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.UnrecoverableTimeoutException;
import run.chronicle.channel.api.ChannelHeader;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;

public class QueuesChannel implements ChronicleChannel {
    private final ChronicleChannelCfg channelCfg;
    private final String connectionId;
    private final ChannelHeader headerOut;
    private final ChronicleQueue publishQueue;
    private final ChronicleQueue subscribeQueue;
    private final ExcerptTailer tailer;

    public QueuesChannel(ChronicleChannelCfg channelCfg, String connectionId, ChannelHeader channelHeader, ChronicleQueue publishQueue, ChronicleQueue subscribeQueue) {
        this.channelCfg = channelCfg;
        this.connectionId = connectionId;
        headerOut = channelHeader;
        this.publishQueue = publishQueue;
        this.subscribeQueue = subscribeQueue;
        tailer = publishQueue.createTailer(connectionId);
    }

    @Override
    public ChronicleChannelCfg channelCfg() {
        return channelCfg;
    }

    @Override
    public ChannelHeader headerOut() {
        return headerOut;
    }

    @Override
    public ChannelHeader headerIn() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        Closeable.closeQuietly(
                tailer,
                publishQueue.acquireAppender(),
                publishQueue,
                subscribeQueue);
    }

    @Override
    public boolean isClosed() {
        return publishQueue.isClosed() || subscribeQueue.isClosed();
    }

    @Override
    public DocumentContext readingDocument() {
        return tailer.readingDocument();
    }

    @Override
    public DocumentContext writingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueue.acquireAppender().writingDocument(metaData);
    }

    @Override
    public DocumentContext acquireWritingDocument(boolean metaData) throws UnrecoverableTimeoutException {
        return publishQueue.acquireAppender().acquireWritingDocument(metaData);
    }
}
