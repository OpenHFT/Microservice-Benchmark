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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.locks.LockSupport;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with
-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=100000 -Diterations=3000000 -Dbuffered=false
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:            7.42         7.42         7.40         7.40         7.40         0.14
90.0:            7.50         7.50         7.46         7.48         7.46         0.29
99.0:            7.69         7.64         7.66         7.69         7.66         0.42
99.7:            9.62         9.58         9.62         9.62         9.33         2.02
99.9:           12.53        14.35        13.04        12.18        11.50        14.17
99.97:          15.31        15.57        15.28        15.09        15.22         2.08
99.99:          16.93        17.18        16.80        15.98        16.80         4.77
99.997:         19.30        19.36        18.72        17.31        19.23         7.31
worst:         168.70        47.81        23.97        21.86       449.02        92.87
----------------------------------------------------------------------------------------------------

-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=1000000 -Diterations=30000000 -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           18.98        19.04        19.10        19.17        19.23         0.67
90.0:           21.79        21.98        22.11        22.18        22.24         0.77
99.0:           23.97        24.93        25.12        25.44        25.63         1.85
99.7:           25.89        26.66        26.72        26.85        26.91         0.64
99.9:           27.17        27.42        27.49        27.49        27.55         0.31
99.97:          28.32        28.26        28.32        28.19        28.26         0.30
99.99:         425.47        31.84        30.24        29.73        29.79         4.52
99.997:       1345.54       713.73        34.75        34.62        32.16        93.39
99.999:       1550.34       777.22       325.12       127.10        34.24        93.53
99.9997:      1591.30       781.31       479.74       184.58        35.52        93.33
worst:        1628.16       807.94       547.84       243.97        37.31        93.23

 */

public class PerfChronicleServerMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    static final String URL = System.getProperty("url", "tcp://127.0.0.1:1248");
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
                "-Durl=" + URL + " " +
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

        if (URL.contains("localhost")) {
            String cfg = "" +
                    "url: " + URL + "\n" +
                    "microservice: !run.chronicle.channel.EchoingMicroservice { }\n" +
                    "buffered: " + BUFFERED;
            ChronicleServiceMain main = Marshallable.fromString(ChronicleServiceMain.class, cfg);
            Thread serverThread = new Thread(main::run);
            serverThread.setDaemon(true);
            serverThread.start();
        }
        java.net.URL url;
        try {
            url = new URL(URL);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        final ChronicleChannelCfg session = new ChronicleChannelCfg()
                .hostname(url.getHost())
                .port(url.getPort())
                .initiator(true)
                .buffered(BUFFERED)
                .pauserMode(PauserMode.balanced);
        client = ChronicleChannel.newChannel(session, new OkHeader("client"));
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
        }, "last-reader");
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
