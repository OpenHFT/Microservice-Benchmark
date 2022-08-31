package services.openmicro.framework;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.Mocker;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.onoes.ExceptionHandler;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.core.util.ThrowingSupplier;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import services.openmicro.driver.api.Driver;
import services.openmicro.driver.api.Event;
import services.openmicro.driver.api.Producer;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static services.openmicro.driver.api.TestMode.latency;
import static services.openmicro.driver.api.TestMode.throughput;

public class Benchmark implements JLBHTask {
    static final Void turnOffLogs = mustBeFirst();
    static final String WORKLOAD_FILE = System.getProperty("workload", "250kps.yaml");
    static final long RUN_TIME = (long) (Double.parseDouble(System.getProperty("runTime", "5")) * 1000);
    static Workload workload = loadWorkload();
    private static ThrowingSupplier<Driver, IOException> driverSupplier;
    private static NanoSampler publish, toService;
    private static Driver latencyDriver;
    private Producer producer;

    private static Void mustBeFirst() {
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off");
        return null;
    }

    private static Workload loadWorkload() {
        ClassAliasPool.CLASS_ALIASES.addAlias(Workload.class);
        try {
            return Marshallable.fromFile(Workload.class, "workloads/" + WORKLOAD_FILE);
        } catch (IOException e) {
            try {
                return Marshallable.fromFile(Workload.class, WORKLOAD_FILE);
            } catch (IOException ex) {
                throw new IORuntimeException(ex);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Jvm.setPerfExceptionHandler(Mocker.ignored(ExceptionHandler.class));
        final String filename = args.length == 0 ? "driver-chronicle/src/main/resources/chronicle.yaml" : args[0];
        driverSupplier = () -> Marshallable.fromFile(Driver.class, filename);
        System.out.println("workload: " + workload);

        latencyDriver = driverSupplier.get();
        System.out.println("driver: " + (latencyDriver instanceof Marshallable ? latencyDriver : WireType.TEXT.asString(latencyDriver)));
        latencyDriver.close();

        // start with a throughput test
        throughputTest();

        // single threaded latency test
        latencyTest();
    }

    private static void throughputTest() throws IOException {
        System.out.println("Warming up");
        int warmup = 0;
        for (int clients : new int[]{warmup, 32, 24, 16, 12, 8, 6, 4, 3, 2, 1}) {
            throughputTest(clients);
        }
    }

    private static void throughputTest(int clients) throws IOException {
        try (Driver driver = driverSupplier.get()) {
            driver.init(throughput);
            boolean warmup = clients == 0;
            if (warmup)
                clients = 8;

            AtomicLong totalCount = new AtomicLong(0);
            ExecutorService es = Executors.newFixedThreadPool(clients);
            long start = System.currentTimeMillis();
            final Runnable work = () -> {
                long end = start + RUN_TIME;
                AtomicLong count = new AtomicLong(0);
                long written = 0;
                Producer producer = driver.createProducer((Event e) -> count.getAndIncrement());
                do {
                    producer.publishEvent(SystemTimeProvider.INSTANCE.currentTimeNanos());
                    if (++written > count.get() + driver.window())
                        Jvm.pause(1);
                } while (System.currentTimeMillis() < end);
                while (written > count.get())
                    Jvm.pause(1);
                totalCount.addAndGet(written);
            };
            List<Future<?>> futures =
                    IntStream.range(0, clients)
                            .mapToObj(i -> es.submit(work))
                            .collect(Collectors.toList());
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
            long time = System.currentTimeMillis() - start;
            long rate = totalCount.get() * 1_000L / time;
            if (warmup)
                System.out.printf(".... warmed up. %,d/s %n", rate);
            else
                System.out.printf("Throughput for clients: %d, Message rate: %,d per seconds%n", clients, rate);
        }
    }

    static void latencyTest() throws IOException {

        for (int throughput : workload.throughputs) {
            latencyDriver = driverSupplier.get();
            System.out.printf("Latency for throughput: %,d per second%n", throughput);
            JLBHOptions lth = new JLBHOptions()
                    .warmUpIterations(latencyDriver.warmup())
                    .iterations(throughput * workload.duration.getSeconds())
                    .throughput(throughput)
                    .accountForCoordinatedOmission(true)
                    .acquireLock(AffinityLock::acquireCore)
                    // disable as otherwise single GC event skews results heavily
                    .recordOSJitter(false)
                    .skipFirstRun(false)
                    .runs(1)
                    .jlbhTask(new Benchmark());
            JLBH jlbh = new JLBH(lth);
            publish = jlbh.addProbe("publish");
            toService = jlbh.addProbe("toService");
            jlbh.start();
            latencyDriver.close();
        }
    }

    @Override
    public void init(JLBH jlbh) {
        latencyDriver.init(latency);
        producer = latencyDriver.createProducer((Event e) -> {
            toService.sampleNanos(e.transactTimeNS() - e.sendingTimeNS());
            jlbh.sample(System.nanoTime() - e.sendingTimeNS());
        });
        latencyDriver.start();
    }

    @Override
    public void run(long startTimeNS) {
        producer.publishEvent(startTimeNS);
        final long end2 = System.nanoTime();
        publish.sampleNanos(end2 - startTimeNS);
    }
}
