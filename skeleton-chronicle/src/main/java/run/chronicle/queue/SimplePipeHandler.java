package run.chronicle.queue;


import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wires;
import run.chronicle.queue.impl.ClosedIORuntimeException;

public class SimplePipeHandler extends SelfDescribingMarshallable implements BrokerHandler {

    private String publish;

    private String subscribe;

    private boolean buffered;

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
    public void run(Marshallable context, Connection connection) {
        Pauser pauser = Pauser.busy();//balancedUpToMillis(1);

        Thread reader = new Thread(() -> reader(pauser, connection), "reader");
        reader.setDaemon(true);
        reader.start();

        try (AffinityLock lock = AffinityLock.acquireLock();
             ChronicleQueue publishQ = single(publish);
             ExcerptAppender appender = publishQ.acquireAppender()) {

            while (!connection.isClosed()) {
                try (DocumentContext dc = connection.readingDocument()) {
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
                    if (!connection.isClosed())
                        Jvm.warn().on(getClass(), e);
                    break;
                }
            }
        } catch (ClosedIORuntimeException e) {
            Jvm.warn().on(getClass(), e.toString());
        }
    }

    private void peek(DocumentContext dc, String prefix) {
        long pos = dc.wire().bytes().readPosition();
        dc.wire().bytes().readSkip(-4);
        try {
            System.out.println(prefix + Wires.fromSizePrefixedBlobs(dc.wire()));
        } finally {
            dc.wire().bytes().readPosition(pos);
        }
    }

    private void reader(Pauser pauser, Connection connection) {
        try (AffinityLock lock = AffinityLock.acquireLock();
             ChronicleQueue subscribeQ = single(subscribe);
             ExcerptTailer tailer = subscribeQ.createTailer().toStart()) {
            while (!connection.isClosed()) {
                try (DocumentContext dc = tailer.readingDocument()) {
                    if (!dc.isPresent()) {
                        pauser.pause();
                        continue;
                    }
                    if (dc.isMetaData()) {
                        continue;
                    }
//                    peek(dc, "O ");
                    try (DocumentContext dc2 = connection.writingDocument()) {
                        dc.wire().copyTo(dc2.wire());
                    }
                    pauser.reset();
                }
            }
        }
    }

    private ChronicleQueue single(String subscribe) {
        return ChronicleQueue.singleBuilder(subscribe).blockSize(OS.isSparseFileSupported() ? 512L << 30 : 64L << 20).build();
    }
}