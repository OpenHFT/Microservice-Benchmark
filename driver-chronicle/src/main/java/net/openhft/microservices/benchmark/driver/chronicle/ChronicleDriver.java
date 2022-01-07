package net.openhft.microservices.benchmark.driver.chronicle;

import net.openhft.affinity.AffinityThreadFactory;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.bytes.MappedBytes;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Maths;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.core.util.ThrowingBiFunction;
import net.openhft.chronicle.queue.BufferMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.Comment;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.microservices.benchmark.driver.api.Driver;
import net.openhft.microservices.benchmark.driver.api.Event;
import net.openhft.microservices.benchmark.driver.api.Producer;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static net.openhft.chronicle.wire.WireType.JSON;

public class ChronicleDriver extends SelfDescribingMarshallable implements Driver {
    // overlap is 1/4 of the total ring buffer size
    static final double BUFFER_TO_OVERLAP_RATIO = 1.0 / 4.0;
    static final int CACHE_LINE_SIZE = 64;
    static final int BASE_HEADER_SIZE = CACHE_LINE_SIZE * 2;

    ChronicleEvent event;
    String path;
    ChronicleEventHandler service;
    BufferMode bufferMode = BufferMode.None;
    @Comment("in MB")
    long bufferCapacity = 128;
    String bufferPath = "/dev/shm";
    transient ExecutorService executor = Executors.newCachedThreadPool(
            new AffinityThreadFactory("microservice"));
    transient SingleChronicleQueue queue1, queue2;
    transient ChronicleEventHandler eventHandler1, eventHandler2;
    transient MethodReader reader1;
    volatile boolean running = true;

    @UsedViaReflection
    static long sizeFor(long capacity, int numReaders) {
        long ringBufferCapacity = Maths.nextPower2(capacity, OS.pageSize());
        long overlap = (long) (ringBufferCapacity * BUFFER_TO_OVERLAP_RATIO);
        return ringBufferCapacity + overlap + headerSize(numReaders);
    }

    static int headerSize(int numReaders) {
        return BASE_HEADER_SIZE + numReaders * CACHE_LINE_SIZE;
    }

    @Override
    public void init() {
        queue2 = createQueue("two");
        eventHandler2 = queue2.acquireAppender()
                .methodWriter(ChronicleEventHandler.class);

        service = new ChronicleMicroservice(eventHandler2);

        queue1 = createQueue("one");
        eventHandler1 = queue1.acquireAppender().methodWriter(ChronicleEventHandler.class);
        reader1 = queue1.createTailer().methodReader(service);
        System.out.println("Event size in JSON: " + JSON.asString(event).length());
    }

    private SingleChronicleQueue createQueue(String name) {
        final File path2 = new File(this.path, name);
        IOTools.deleteDirWithFiles(path2);
        return ChronicleQueue.singleBuilder(path2)
                .useSparseFiles(true)
                .readBufferMode(bufferMode)
                .writeBufferMode(bufferMode)
                .bufferBytesStoreCreator(bufferBytesStoreCreatorForTopic(name))
                .build();
    }

    private ThrowingBiFunction<Long, Integer, BytesStore, Exception> bufferBytesStoreCreatorForTopic(String topic) {
        return (capacity, maxTailers) -> {
            long length = sizeFor(bufferCapacity << 20, 2); // MB
            final File file = new File(bufferPath, "buffer-" + topic);
            Files.deleteIfExists(file.toPath());
            return MappedBytes.singleMappedBytes(file, length);
        };
    }

    @Override
    public void start() {
        executor.submit(this::microserviceRunner);
        System.out.println("Started " + this);
    }

    private void microserviceRunner() {
        Pauser pauser = Pauser.busy();
        while (running) {
            if (reader1.readOne())
                pauser.reset();
            else
                pauser.pause();
        }
        Closeable.closeQuietly(queue1);
    }

    @Override
    public Producer createProducer(Consumer<Event> eventConsumer) {
        final ChronicleEventHandler chronicleEventHandler = eventConsumer::accept;
        final MethodReader reader2 = queue2.createTailer().methodReader(chronicleEventHandler);
        executor.submit(() -> readReplies(reader2));

        // send the first message
        return s -> {
            event.sendingTimeNS(s);
            event.transactTimeNS(0L);
            eventHandler1.event(event);
        };
    }

    private void readReplies(MethodReader reader2) {
        Pauser pauser = Pauser.busy();
        while (running) {
            if (reader2.readOne())
                pauser.reset();
            else
                pauser.pause();
        }

        Closeable.closeQuietly(queue2);
    }

    @Override
    public void close() {
        Driver.super.close();
        running = false;
    }
}
