package run.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.Marshallable;
import run.chronicle.queue.impl.ClosedIORuntimeException;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with
-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=100000 -Diterations=3000000
-------------------------------- SUMMARY (end to end) us -------------------------------------------
50.0:           19.17        19.17        19.17        19.10        19.17         0.22
90.0:           27.87        27.87        27.87        27.87        27.87         0.00
99.0:           37.82        37.57        37.57        37.57        37.44         0.23
99.7:           48.32        47.30        47.42        46.91        46.78         0.90
99.9:           51.90        51.01        51.65        51.26        51.14         0.83
99.97:          67.97        61.25        68.99        62.78        63.30         7.77
99.99:          88.19        70.27       455.17        71.55        76.93        78.50
99.997:        513.54        81.79      1103.87        83.33       558.08        89.28
worst:        1103.87       149.76      1415.17       190.72      1107.97        84.92

-Dsize=256 -Dthroughput=200000 -Diterations=6000000 -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           46.53        46.66        46.78        46.78        46.78         0.18
90.0:           56.64        56.51        56.90        56.77        56.38         0.60
99.0:           70.02        69.76        70.02        69.76        68.99         0.98
99.7:           77.18        77.18        77.18        77.44        76.16         1.11
99.9:           85.63        86.91        86.40        87.68        84.10         2.76
99.97:          94.85       109.70        96.90       349.70        94.34        64.34
99.99:         104.06      1173.50       142.59      1992.70       111.23        91.85
99.997:        220.42      1710.08       662.53      2748.42       592.90        70.79
worst:         458.24      2142.21      1234.94      3543.04      1361.92        55.48

-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=500000 -Diterations=15000000 -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           50.75        50.75        50.88        51.01        51.14         0.50
90.0:           62.91        62.78        62.91        63.17        63.30         0.54
99.0:           83.84        82.30        82.56        83.58        83.84         1.23
99.7:           97.15        92.80        93.06        95.36        95.10         1.81
99.9:          404.99       102.78       103.81       121.22       110.98        10.68
99.97:        1419.26       212.74       341.50      1043.46      1247.23        76.43
99.99:        1955.84       842.75      1041.41      1824.77      2082.82        49.52
99.997:       2338.82      1107.97      2054.14      2347.01      2478.08        45.19
99.999:       2543.62      1259.52      2437.12      2560.00      2748.42        44.07
worst:        2666.50      1370.11      2658.30      2674.69      2969.60        43.77
 */

public class PerfChronicleGatewayMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    static final String HOSTNAME = System.getProperty("hostname", "localhost");
    private Data data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private Connection client, server;
    private volatile boolean complete;
    private int sent;
    private volatile int count;
    private boolean warmedUp;
    private ChronicleGatewayMain main;

    public static void main(String[] args) {
        System.out.println("" +
                "-Dhostname=" + HOSTNAME + " " +
                "-Dsize=" + SIZE + " " +
                "-Dthroughput=" + THROUGHPUT + " " +
                "-Diterations=" + ITERATIONS + " " +
                "-Dbuffered=" + BUFFERED);

        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(50_000)
                .iterations(ITERATIONS)
                .throughput(THROUGHPUT)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(true)
                .runs(5)
                .jlbhTask(new PerfChronicleGatewayMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.data = new Data();
        this.data.data = new byte[SIZE - Long.BYTES];

        if (HOSTNAME.equals("localhost")) {
            IOTools.deleteDirWithFiles("echo.in");
            IOTools.deleteDirWithFiles("echo.out");

            String brokerCfg = "" +
                    "port: 65432\n" +
                    "buffered: " + BUFFERED;
            try {
                main = Marshallable.fromString(ChronicleGatewayMain.class, brokerCfg).start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        final ConnectionCfg session = new ConnectionCfg()
                .hostname(HOSTNAME)
                .port(65432)
                .initiator(true)
                .buffered(BUFFERED)
                .pauser(PauserMode.balanced);

        server = Connection.createFor(session, new SimplePipeHandler("server").subscribe("echo.in").publish("echo.out"));
        Thread serverThread = new Thread(() -> runServer(server, Echoed.class, EchoingMicroservice::new), "server");
        serverThread.setDaemon(true);
        serverThread.start();

        client = Connection.createFor(session, new SimplePipeHandler("client").subscribe("echo.out").publish("echo.in"));

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
        }, "last-reader");
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private <OUT, MS> void runServer(Connection server, Class<OUT> outClass, Function<OUT, MS> serviceConstructor) {
        final OUT out = server.methodWriter(outClass);
        final MS ms = serviceConstructor.apply(out);
        MethodReader reader = server.methodReader(ms);
        try (AffinityLock lock = AffinityLock.acquireLock()) {
            while (!server.isClosed()) {
                reader.readOne();
            }
        } catch (ClosedIORuntimeException closed) {
            Jvm.warn().on(getClass(), closed.toString());
        }
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
        server.close();
        Closeable.closeQuietly(main);
    }
}

