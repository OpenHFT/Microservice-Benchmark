package run.chronicle.queue;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.bytes.MethodReader;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.threads.PauserMode;
import net.openhft.chronicle.wire.Marshallable;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/*
Ryzen 9 5950X with Ubuntu 21.10 run with -Dsize=256 -Dthroughput=1000000 -Diterations=30000000 -Dbuffered
-------------------------------- SUMMARY (end to end) us -------------------------------------------


 */

public class PerfChronicleBrokerMain implements JLBHTask {
    static final int THROUGHPUT = Integer.getInteger("throughput", 100_000);
    static final int ITERATIONS = Integer.getInteger("iterations", THROUGHPUT * 30);
    static final int SIZE = Integer.getInteger("size", 256);
    static final boolean BUFFERED = Jvm.getBoolean("buffered");
    private Data data;
    private Echoing echoing;
    private MethodReader reader;
    private Thread readerThread;
    private Connection client, server;
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
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(true)
                .runs(5)
                .jlbhTask(new PerfChronicleBrokerMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.data = new Data();
        this.data.data = new byte[SIZE - Long.BYTES];

        String brokerCfg = "" +
                "port: 65432\n" +
                "buffered: " + BUFFERED;
        ChronicleBrokerMain main = Marshallable.fromString(ChronicleBrokerMain.class, brokerCfg);
        Thread brokerThread = new Thread(main::run, "broker");
        brokerThread.setDaemon(true);
        brokerThread.start();

        final ConnectionCfg session = new ConnectionCfg()
                .hostname("localhost")
                .port(65432)
                .initiator(true)
                .buffered(BUFFERED)
                .pauser(PauserMode.balanced);

        server = Connection.createFor(session, new SimplePipeHandler().subscribe("echo.in").publish("echo.out"));
        Thread serverThread = new Thread(() -> runServer(server, Echoed.class, EchoingMicroservice::new), "server");
        serverThread.setDaemon(true);
        serverThread.start();

        client = Connection.createFor(session, new SimplePipeHandler().subscribe("echo.out").publish("echo.in"));

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

    private <OUT, MS> void runServer(Connection server, Class<OUT> outClass, Function<OUT, MS> serviceConstructor) {
        final OUT out = server.methodWriter(outClass);
        final MS ms = serviceConstructor.apply(out);
        MethodReader reader = server.methodReader(ms);
        try (AffinityLock lock = AffinityLock.acquireCore()) {
            while (!server.isClosed()) {
                reader.readOne();
            }
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
    }
}

