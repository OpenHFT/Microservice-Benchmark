package services.openmicro.framework;

import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.core.pool.ClassAliasPool;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.WireType;
import services.openmicro.driver.api.Driver;
import services.openmicro.driver.api.Event;
import services.openmicro.driver.api.Producer;

import java.io.IOException;

public class Benchmark implements JLBHTask {
    static final String WORKLOAD_FILE = System.getProperty("workload", "250kps.yaml");
    static Workload workload = loadWorkload();
    private static Driver driver;
    private static NanoSampler publish, toService;
    private Producer producer;

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
        driver = Marshallable.fromFile(Driver.class, args.length == 0 ? "driver-chronicle/src/main/resources/chronicle.yaml" : args[0]);
        System.out.println("workload: " + workload);

        final int runs = 5;
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(driver.warmup())
                .iterations(workload.throughput * workload.duration.toSeconds() / runs)
                .throughput(workload.throughput)
                .accountForCoordinatedOmission(true)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .skipFirstRun(false)
                .runs(runs)
                .jlbhTask(new Benchmark());
        JLBH jlbh = new JLBH(lth);
        publish = jlbh.addProbe("publish");
        toService = jlbh.addProbe("toService");
        jlbh.start();
    }

    @Override
    public void init(JLBH jlbh) {
        driver.init();
        producer = driver.createProducer((Event e) -> {
            toService.sampleNanos(e.transactTimeNS() - e.sendingTimeNS());
            jlbh.sample(System.nanoTime() - e.sendingTimeNS());
        });
        driver.start();
        System.out.println("driver: " + (driver instanceof Marshallable ? driver : WireType.TEXT.asString(driver)));
    }

    @Override
    public void run(long startTimeNS) {
        producer.publishEvent(startTimeNS);
        final long end2 = System.nanoTime();
        publish.sampleNanos(end2 - startTimeNS);
    }

    @Override
    public void complete() {
        JLBHTask.super.complete();
        driver.close();
    }
}
