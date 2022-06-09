package run.chronicle.channel;


import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.NanoTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleContext;
import run.chronicle.channel.api.SystemContext;
import run.chronicle.channel.impl.BufferedChronicleChannel;
import run.chronicle.channel.impl.ClosedIORuntimeException;

public class PipeHandler extends SelfDescribingMarshallable implements ChannelHandler {
    private SystemContext systemContext = SystemContext.INSTANCE;

    private String connectionId;

    private String publish;
    private String subscribe;

    private boolean buffered;

    public PipeHandler() {
        this(NanoTimestampLongConverter.INSTANCE.asString(
                SystemTimeProvider.CLOCK.currentTimeNanos()));
    }

    public PipeHandler(String connectionId) {
        this.connectionId = connectionId;
    }

    public PipeHandler flip() {
        return new PipeHandler().publish(subscribe()).subscribe(publish());
    }

    @Override
    public SystemContext systemContext() {
        return systemContext;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    public PipeHandler connectionId(String connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    public String publish() {
        return publish;
    }

    public PipeHandler publish(String publish) {
        this.publish = publish;
        return this;
    }

    public String subscribe() {
        return subscribe;
    }

    public PipeHandler subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        ChronicleQueue subscribeQ = null;
        final ExcerptTailer tailer;

        if (channel instanceof BufferedChronicleChannel) {
            BufferedChronicleChannel bc = (BufferedChronicleChannel) channel;
            subscribeQ = single(subscribe);
            final String id = channel.headerIn().connectionId();
            tailer = subscribeQ.createTailer(id).toStart();
            bc.eventPoller(conn -> {
                boolean wrote = false;
                while (copyOneMessage(conn, tailer))
                    wrote = true;
                return wrote;
            });
        } else {
            Thread tailerThread = new Thread(() -> queueTailer(pauser, channel), connectionId + "~tailer");
            tailerThread.setDaemon(true);
            tailerThread.start();

            tailer = null;
        }

        Thread.currentThread().setName(connectionId + "~reader");
        try (AffinityLock lock = AffinityLock.acquireLock();
             ChronicleQueue publishQ = single(publish);
             ExcerptAppender appender = publishQ.acquireAppender()) {

            while (!channel.isClosed()) {
                try (DocumentContext dc = channel.readingDocument()) {
                    pauser.unpause();

                    if (!dc.isPresent()) {
                        continue;
                    }
                    if (dc.isMetaData()) {
                        // read message
                        continue;
                    }

                    try (DocumentContext dc2 = appender.writingDocument()) {
                        dc.wire().copyTo(dc2.wire());
                    }
                } catch (ClosedIORuntimeException e) {
                    if (!channel.isClosed())
                        Jvm.warn().on(getClass(), e);
                    break;
                }
            }

        } catch (ClosedIORuntimeException e) {
            Jvm.warn().on(getClass(), e.toString());

        } finally {
            Closeable.closeQuietly(tailer, subscribeQ);
            Thread.currentThread().setName("connections");
        }
    }

    private void queueTailer(Pauser pauser, ChronicleChannel channel) {
        try (AffinityLock lock = AffinityLock.acquireLock();
             ChronicleQueue subscribeQ = single(subscribe);
             ExcerptTailer tailer = subscribeQ.createTailer().toStart()) {
            while (!channel.isClosed()) {
                if (copyOneMessage(channel, tailer))
                    pauser.reset();
                else
                    pauser.pause();
            }
        }
    }

    private boolean copyOneMessage(ChronicleChannel channel, ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return false;
            }

            final long dataBuffered;
            try (DocumentContext dc2 = channel.writingDocument()) {
                dc.wire().copyTo(dc2.wire());

                dataBuffered = dc2.wire().bytes().writePosition();
            }
            // wait for it to drain
            return dataBuffered < 32 << 10;
        }
    }

    private ChronicleQueue single(String subscribe) {
        return ChronicleQueue.singleBuilder(subscribe).blockSize(OS.isSparseFileSupported() ? 512L << 30 : 64L << 20).build();
    }
}