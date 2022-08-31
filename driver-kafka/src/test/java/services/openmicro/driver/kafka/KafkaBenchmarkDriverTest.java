package services.openmicro.driver.kafka;


import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Marshallable;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KafkaBenchmarkDriverTest {

    @Test
    public void bytesToEvent() throws IOException {
        String str =
                "!services.openmicro.driver.kafka.ChronicleEvent {\n" +
                        "  sendingTimeNS: 0,\n" +
                        "  transactTimeNS: 0,\n" +
                        "  dateTime1: 2022-01-06T11:00:00,\n" +
                        "  dateTime2: 2022-01-06T12:00:00,\n" +
                        "  dateTime3: 2022-01-07T11:11:11.111222,\n" +
                        "  dateTime4: 2022-01-07T11:11:21.509977,\n" +
                        "  text1: short,\n" +
                        "  text2: longer,\n" +
                        "  text3: a bit longer than that,\n" +
                        "  text4: \"Sphinx of black quartz, judge my vow\",\n" +
                        "  number1: 1,\n" +
                        "  number2: 12345,\n" +
                        "  number3: 123456789012,\n" +
                        "  number4: 876543210123456789,\n" +
                        "  value1: 0.0,\n" +
                        "  value2: 1.2345,\n" +
                        "  value3: 1E6,\n" +
                        "  value4: 12345678.9,\n" +
                        "  value5: 0.001,\n" +
                        "  value6: 6.0,\n" +
                        "  value7: 1000000E6,\n" +
                        "  value8: 8888.8888\n" +
                        "}\n";
        final ChronicleEvent event1 = Marshallable.fromString(ChronicleEvent.class, str);
        Bytes bytes = Bytes.allocateElasticOnHeap();
        KafkaBenchmarkDriver.eventToBytes(bytes, event1);

        ChronicleEvent event = new ChronicleEvent();
        KafkaBenchmarkDriver.bytesToEvent(bytes.toByteArray(), event);
        assertEquals(str, event.toString());
    }
}