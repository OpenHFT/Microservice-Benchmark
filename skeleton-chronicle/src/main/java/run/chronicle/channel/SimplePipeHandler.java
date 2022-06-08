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
import run.chronicle.channel.api.Channel;
import run.chronicle.channel.api.ChannelHandler;
import run.chronicle.channel.api.ChronicleContext;
import run.chronicle.channel.api.SystemContext;
import run.chronicle.channel.impl.BufferedChannel;
import run.chronicle.channel.impl.ClosedIORuntimeException;

public class SimplePipeHandler extends SelfDescribingMarshallable implements ChannelHandler {
    private SystemContext systemContext = SystemContext.INSTANCE;

    private String connectionId;

    private String publish;
    private String subscribe;

    private boolean buffered;

    public SimplePipeHandler() {
        this(NanoTimestampLongConverter.INSTANCE.asString(
                SystemTimeProvider.CLOCK.currentTimeNanos()));
    }

    public SimplePipeHandler(String connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public SystemContext systemContext() {
        return systemContext;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    public SimplePipeHandler connectionId(String connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    public String publish() {
        return publish;
    }

    public SimplePipeHandler publish(String publish) {
        this.publish = publish;
        return this;
    }

    public String subscribe() {
        return subscribe;
    }

    public SimplePipeHandler subscribe(String subscribe) {
        this.subscribe = subscribe;
        return this;
    }

    @Override
    public void run(ChronicleContext context, Channel channel) {
        Pauser pauser = Pauser.balanced();

        ChronicleQueue subscribeQ = null;
        final ExcerptTailer tailer;

        if (channel instanceof BufferedChannel) {
            BufferedChannel bc = (BufferedChannel) channel;
            subscribeQ = single(subscribe);
            tailer = subscribeQ.createTailer().toStart();
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

//                    peek(dc, "I ");
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

/*    private void peek(DocumentContext dc, String prefix) {
        long pos = dc.wire().bytes().readPosition();
        dc.wire().bytes().readSkip(-4);
        try {
            System.out.println(prefix + Wires.fromSizePrefixedBlobs(dc.wire()));
        } finally {
            dc.wire().bytes().readPosition(pos);
        }
    }*/

    private void queueTailer(Pauser pauser, Channel channel) {
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

    private boolean copyOneMessage(Channel channel, ExcerptTailer tailer) {
        try (DocumentContext dc = tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return false;
            }
//                    peek(dc, "O ");
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