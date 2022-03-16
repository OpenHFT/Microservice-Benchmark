package run.chronicle.queue;

import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.MethodId;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.annotation.UsedViaReflection;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.BytesInBinaryMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.util.concurrent.locks.LockSupport;

interface Echoing {
    @MethodId(1)
    void echo(Data data);
}

interface Echoed {
    @MethodId(2)
    void echoed(Data data);
}

public class PerfChronicleServerMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    private Data data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private Connection client;
    private volatile boolean complete;
    private int sent;
    private volatile int count;

    public static void main(String[] args) {
        System.out.println("" +
                "-Dthroughput=" + THROUGHPUT + " " +
                "-Diterations=" + ITERATIONS);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(true)
                .runs(5)
                .jlbhTask(new PerfChronicleServerMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.data = new Data();
        this.data.data = new byte[SIZE - Long.BYTES];

        String cfg = "" +
                "port: 65432\n" +
                "microservice: !run.chronicle.queue.EchoingMicroservice { }";
        ChronicleServerMain main = Marshallable.fromString(ChronicleServerMain.class, cfg);
        Thread serverThread = new Thread(main::run);
        serverThread.setDaemon(true);
        serverThread.start();

        client = Connection.createFor(new SessionCfg().hostname("localhost").port(65432).initiator(true).buffered(true).pauser(PauserMode.busy));
        echoing = client.methodWriter(Echoing.class);
        reader = client.methodReader(new Echoed() {
            @Override
            public void echoed(Data data) {
                jlbh.sample(System.nanoTime() - data.timeNS);
                count++;
            }
        });
        readerThread = new Thread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    reader.readOne();
                }
            } catch (Throwable t) {
                if (!complete)
                    t.printStackTrace();
            }
        });
        readerThread.setDaemon(true);
        readerThread.start();
    }

    @Override
    public void warmedUp() {
        JLBHTask.super.warmedUp();
    }

    @Override
    public void run(long startTimeNS) {
        data.timeNS = startTimeNS;
        echoing.echo(data);
        long lag = sent++ - count;
        if (lag >= 50)
            LockSupport.parkNanos(lag * 500L);
    }

    @Override
    public void complete() {
        this.complete = true;
        readerThread.interrupt();
        client.close();
    }
}

class Data extends BytesInBinaryMarshallable {
    long timeNS;
    byte[] data;

    @Override
    public void readMarshallable(BytesIn bytes) throws IORuntimeException, BufferUnderflowException, IllegalStateException {
        timeNS = bytes.readLong();
        int len = bytes.readUnsignedShort();
        if (data == null || data.length != len)
            data = new byte[len];
        bytes.read(data);
    }

    @Override
    public void writeMarshallable(BytesOut bytes) throws IllegalStateException, BufferOverflowException, BufferUnderflowException, ArithmeticException {
        bytes.writeLong(timeNS);
        bytes.writeUnsignedShort(data.length);
        bytes.write(data);
    }
}

@UsedViaReflection
class EchoingMicroservice extends SelfDescribingMarshallable implements Closeable, Echoing {
    transient MicroserviceOut<Echoed, Void> out;
    transient boolean closed;

    public EchoingMicroservice(MicroserviceOut<Echoed, Void> out) {
        this.out = out;
    }

    @Override
    public void echo(Data data) {
        out.out().echoed(data);
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
