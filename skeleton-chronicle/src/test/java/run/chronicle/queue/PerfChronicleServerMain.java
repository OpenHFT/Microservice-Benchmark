package run.chronicle.queue;

import net.openhft.affinity.AffinityLock;
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

/*
Ryzen 9 5950X with Ubuntu 21.10 run with -Dsize=256 -Dthroughput=100000 -Diterations=3000000
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:            6.84         6.84         6.82         6.86         6.84         0.31
90.0:            6.92         6.94         6.92         6.94         6.92         0.15
99.0:            7.03         7.05         7.05         7.05         7.02         0.30
99.7:            8.10         8.15         8.07         8.24         8.34         2.13
99.9:            9.68         9.78         9.62        10.13        10.48         5.65
99.97:          14.19        14.35        14.19        14.90        15.06         3.90
99.99:          15.73        16.08        15.92        16.74        16.99         4.30
99.997:         17.18        17.70        17.31        18.40        18.59         4.70
worst:          61.12        63.81        54.59        83.58        26.21        59.34
----------------------------------------------------------------------------------------------------
 */

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
    private boolean warmedUp;

    public static void main(String[] args) {
        System.out.println("" +
                "-Dsize=" + SIZE + " " +
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

        final SessionCfg session = new SessionCfg().hostname("localhost").port(65432).initiator(true).buffered(false).pauser(PauserMode.balanced);
        client = Connection.createFor(session, new SimpleHeader());
        echoing = client.methodWriter(Echoing.class);
        reader = client.methodReader((Echoed) data -> {
            jlbh.sample(System.nanoTime() - data.timeNS);
            count++;
        });
        readerThread = new Thread(() -> {
            try (AffinityLock lock = AffinityLock.acquireLock()) {
                while (!Thread.currentThread().isInterrupted()) {
                    reader.readOne();
                }
            } catch (Throwable t) {
                if (!complete)
                    t.printStackTrace();
            }
        }, "reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    @Override
    public void warmedUp() {
        JLBHTask.super.warmedUp();
        warmedUp = true;
    }

    @Override
    public void run(long startTimeNS) {
        data.timeNS = startTimeNS;
        echoing.echo(data);

        // throttling when warming up.
        if (!warmedUp) {
            long lag = sent++ - count;
            if (lag >= 50)
                LockSupport.parkNanos(lag * 500L);
        }
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
