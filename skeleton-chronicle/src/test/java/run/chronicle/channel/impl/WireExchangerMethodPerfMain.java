package run.chronicle.channel.impl;

import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.jlbh.JLBH;
import net.openhft.chronicle.jlbh.JLBHOptions;
import net.openhft.chronicle.jlbh.JLBHTask;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.Wire;

/*
on Windows laptop without affinity
-------------------------------- SUMMARY (end to end) ----------------------------------
Percentile   run1         run2         run3         run4         run5      % Variation
50.0:            0.30         0.30         0.30         0.30         0.30         0.00
90.0:            0.30         0.30         0.30         0.30         0.30         0.00
99.0:            0.40         0.40         0.40         0.30         0.30        18.16
99.7:            2.50         3.10         3.10         1.90         2.00        29.57
99.9:           16.11        17.31        16.42        15.50        15.22         8.41
99.97:          24.42        31.71        24.48        22.30        21.60        23.79
99.99:          40.26      1865.73       110.46        31.78        28.70        97.71
99.997:        129.66      3133.44       623.62        64.32        40.38        98.08
99.999:        235.78      3493.89      1043.46       197.38       109.18        95.38
worst:         522.75      3903.49      1239.04       335.36       248.06        90.76
 */

public class WireExchangerMethodPerfMain implements JLBHTask {

    private static int warmup = 100_000;
    private static int iterations = 10_000_000;
    private static int throughput = 500_000;
    private JLBH jlbh;
    private WireExchanger be = new WireExchanger();
    private ChronicleEvent event;
    private EventListener eventListener;

    public static void main(String[] args) {
        JLBHOptions lth = new JLBHOptions()
                .warmUpIterations(warmup)
                .iterations(iterations)
                .throughput(throughput)
                .acquireLock(AffinityLock::acquireCore)
                // disable as otherwise single GC event skews results heavily
                .recordOSJitter(false)
                .accountForCoordinatedOmission(true)
                .skipFirstRun(false)
                .runs(5)
                .jlbhTask(new WireExchangerMethodPerfMain());
        new JLBH(lth).start();
    }

    @Override
    public void init(JLBH jlbh) {
        this.jlbh = jlbh;
        final Thread consumer = new Thread(this::run, "consumer");
        consumer.setDaemon(true);
        consumer.start();
        event = Marshallable.fromString(ChronicleEvent.class, "{\n" +
                "    dateTime1: 2022-01-06T11:00:00,\n" +
                "    dateTime2: 2022-01-06T12:00:00,\n" +
                "    dateTime3: 2022-01-07T11:11:11.111222333,\n" +
                "    dateTime4: 2022-01-07T11:11:21.509977,\n" +
                "    text1: \"short\",\n" +
                "    text2: \"longer\",\n" +
                "    text3: \"a bit longer than that\",\n" +
                "    text4: \"Sphinx of black quartz, judge my vow\",\n" +
                "    number1: 1,\n" +
                "    number2: 12345,\n" +
                "    number3: 123456789012,\n" +
                "    number4: 876543210123456789,\n" +
                "    value1: 0.0,\n" +
                "    value2: 1.2345,\n" +
                "    value3: 1000000,\n" +
                "    value4: 12345678.9,\n" +
                "    value5: 0.001,\n" +
                "    value6: 6.0,\n" +
                "    value7: 1e12,\n" +
                "    value8: 8888.8888\n" +
                "  }");
        eventListener = be.methodWriter(EventListener.class);
    }

    private void run() {
        int count = 0;
        ChronicleEvent event2 = new ChronicleEvent();
        try (AffinityLock lock = AffinityLock.acquireCore()) {
            while (!Thread.currentThread().isInterrupted()) {
                final Wire wire = be.acquireConsumer();
                while (!wire.isEmpty()) {
                    try (DocumentContext dc = wire.readingDocument()) {
                        final ChronicleEvent event2b = dc.wire().read().object(event2, ChronicleEvent.class);
                        if (event2b.transactTimeNS() != count)
                            throw new AssertionError("Missed a message, expected " + count + " got " + event2.transactTimeNS());
                        final long durationNs = System.nanoTime() - event2b.sendingTimeNS();
                        jlbh.sample(durationNs);
                        ++count;
                    }
                }
                wire.clear();
                be.releaseConsumer();
            }
        }
    }

    int count = 0;

    @Override
    public void run(long startTimeNS) {
        event.sendingTimeNS(startTimeNS);
        event.transactTimeNS(count++);
        eventListener.event(event);
    }
}
