package run.chronicle.channel;

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
import run.chronicle.channel.api.ChronicleChannel;
import run.chronicle.channel.api.ChronicleChannelCfg;
import run.chronicle.channel.impl.ClosedIORuntimeException;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with
-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=50000 -Diterations=1500000 -DpauseMode=balanced -Dbuffered=false
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           15.95        15.95        15.89        15.95        15.95         0.27
90.0:           17.82        17.82        17.82        17.89        17.82         0.24
99.0:           26.21        25.89        25.44        25.44        25.12         2.00
99.7:           45.89        41.15        38.98        39.10        38.46         4.45
99.9:           49.47        49.34        48.58        48.70        48.58         1.04
99.97:          51.90        51.78        49.60        49.73        49.60         2.84
99.99:          69.50        62.02        55.62        53.18        57.28         9.97
worst:         906.24       335.36       333.31       129.92       376.32        55.84

-Dhostname=127.0.0.1 -Dsize=256 -Dthroughput=500000 -Diterations=15000000 -DpauserMode=busy -Dbuffered=true
-------------------------------- SUMMARY (end to end) us -------------------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:           48.06        47.55        47.04        47.17        47.55         0.72
90.0:           60.22        58.82        57.15        57.28        58.43         1.90
99.0:           78.98        74.88        70.53        70.78        73.09         3.95
99.7:           93.57        88.19        80.26        80.77        84.10         6.18
99.9:          272.90       371.20       112.51       168.19       117.63        60.52
99.97:        1054.72       988.16       599.04       654.34       689.15        30.22
99.99:        1533.95      1398.78       908.29       914.43      1132.54        26.47
99.997:       1845.25      1648.64      1120.26      1054.72      1472.51        27.29
99.999:       2033.66      1796.10      1222.66      1132.54      1656.83        28.09
worst:        2191.36      1951.74      1366.02      1210.37      1837.06        28.99
 */

public class PerfChronicleGatewayMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    static final String HOSTNAME = System.getProperty("hostname", "localhost");
    private static final PauserMode PAUSER_MODE = PauserMode.valueOf(System.getProperty("pauserMode", PauserMode.balanced.name()));
    private Data data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private ChronicleChannel client, server;
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
                "-DpauseMode=" + PAUSER_MODE + " " +
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

        final ChronicleChannelCfg channelCfg = new ChronicleChannelCfg()
                .hostname(HOSTNAME)
                .port(65432)
                .initiator(true)
                .buffered(BUFFERED)
                .pauserMode(PAUSER_MODE);

        server = ChronicleChannel.newChannel(channelCfg, new PipeHandler("server").subscribe("echo.in").publish("echo.out"));
        Thread serverThread = new Thread(() -> runServer(server, Echoed.class, EchoingMicroservice::new), "server");
        serverThread.setDaemon(true);
        serverThread.start();

        client = ChronicleChannel.newChannel(channelCfg, new PipeHandler("client").subscribe("echo.out").publish("echo.in"));

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

    private <OUT, MS> void runServer(ChronicleChannel server, Class<OUT> outClass, Function<OUT, MS> serviceConstructor) {
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

