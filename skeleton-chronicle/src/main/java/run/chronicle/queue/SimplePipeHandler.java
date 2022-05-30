package run.chronicle.queue;


import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

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

        try (ChronicleQueue subscribeQ = ChronicleQueue.single(subscribe);
             ExcerptTailer tailer = subscribeQ.createTailer();
             ChronicleQueue publishQ = ChronicleQueue.single(publish);
             ExcerptAppender appender = publishQ.acquireAppender()) {

            PipeState ps = PipeState.CONNECTION_READING;

            while (!connection.isClosed()) {
                switch (ps) {
                    case CONNECTION_READING:
                        try (DocumentContext dc = connection.readingDocument()) {
                            if (!dc.isPresent()) {
                                ps = PipeState.QUEUE_READING;
                                continue;
                            }
                            if (dc.isMetaData()) {
                                // read message
                                ps = PipeState.QUEUE_READING;
                                continue;
                            }

                            try (DocumentContext dc2 = appender.writingDocument()) {
                                dc.wire().copyTo(dc2.wire());
                            }

                            if (!buffered)
                                ps = PipeState.QUEUE_READING;
                        }
                        break;

                    case QUEUE_READING:
                        try (DocumentContext dc2 = tailer.readingDocument()) {
                            if (!dc2.isPresent()) {
                                ps = PipeState.CONNECTION_READING;
                                continue;
                            }
                            try (DocumentContext dc3 = connection.writingDocument()) {
                                dc2.wire().copyTo(dc3.wire());
                            }
                        }
                        break;

                    default:
                        throw new IllegalStateException("Unexpected value: " + ps);
                }
            }
        }
    }

    enum PipeState {
        CONNECTION_READING,
        QUEUE_READING
    }
}
