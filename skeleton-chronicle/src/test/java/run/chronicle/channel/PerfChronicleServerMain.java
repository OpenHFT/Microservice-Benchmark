package run.chronicle.channel;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.Marshallable;
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;

import java.util.concurrent.locks.LockSupport;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with -Dsize=256 -Dthroughput=1000000 -Diterations=30000000 -Dbuffered
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           16.93        16.99        16.99        16.99        16.99         0.00
90.0:           19.68        19.74        19.74        19.74        19.68         0.22
99.0:           24.42        24.61        24.22        24.10        23.97         1.75
99.7:           25.76        25.89        25.82        25.76        25.76         0.33
99.9:           26.46        26.66        26.66        26.66        26.66         0.00
99.97:          27.17        27.30        27.30        27.36        27.30         0.16
99.99:          27.62        27.74        27.68        27.74        27.68         0.15
99.997:         28.96        88.19        28.19        28.38        28.32        58.66
99.999:        153.86       170.75        29.02        30.05        29.47        76.50
99.9997:       177.41       179.46        71.55        33.47        32.29        75.24
worst:         249.09       185.09       147.71       104.06        37.82        72.19
----------------------------------------------------------------------------------------------------

 */

public class PerfChronicleServerMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    private Data data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private ChronicleChannel client;
    private volatile boolean complete;
    private int sent;
    private volatile int count;
    private boolean warmedUp;

    public static void main(String[] args) {
        System.out.println("" +
                "-Dsize=" + SIZE + " " +
                "-Dthroughput=" + THROUGHPUT + " " +
                "-Diterations=" + ITERATIONS + " " +
                "-Dbuffered=" + BUFFERED);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                .acquireLock(AffinityLock::acquireCore)
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
                "microservice: !run.chronicle.queue.EchoingMicroservice { }" +
                "buffered: " + BUFFERED;
        ChronicleServiceMain main = Marshallable.fromString(ChronicleServiceMain.class, cfg);
        Thread serverThread = new Thread(main::run);
        serverThread.setDaemon(true);
        serverThread.start();

        final ChronicleChannelCfg session = new ChronicleChannelCfg()
                .hostname("localhost")
                .port(65432)
                .initiator(true)
                .buffered(BUFFERED)
                .pauser(PauserMode.balanced);
        client = ChronicleChannel.newChannel(session, new SimpleHandler("client"));
        echoing = client.methodWriter(Echoing.class);
        reader = client.methodReader((Echoed) data -> {
            jlbh.sample(System.nanoTime() - data.timeNS);
            count++;
        });
        readerThread = new Thread(() -> {
            try (AffinityLock lock = AffinityLock.acquireCore()) {
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
