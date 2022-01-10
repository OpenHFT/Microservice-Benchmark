package services.openmicro.driver.kafka;


import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class KafkaBenchmarkDriverTest {

    @Test
    public void bytesToEvent() throws IOException {
        String str = "---\n" +
                "sendingTimeNS: 0\n" +
                "transactTimeNS: 0\n" +
                "dateTime1: \"2022-01-06T11:00:00.000000Z\"\n" +
                "dateTime2: \"2022-01-06T12:00:00.000000Z\"\n" +
                "dateTime3: \"2022-01-07T11:11:11.111222Z\"\n" +
                "dateTime4: \"2022-01-07T11:11:21.509977Z\"\n" +
                "text1: \"short\"\n" +
                "text2: \"longer\"\n" +
                "text3: \"a bit longer than that\"\n" +
                "text4: \"Sphinx of black quartz, judge my vow\"\n" +
                "number1: 1\n" +
                "number2: 12345\n" +
                "number3: 123456789012\n" +
                "number4: 876543210123456789\n" +
                "value1: 0.0\n" +
                "value2: 1.2345\n" +
                "value3: 1000000.0\n" +
                "value4: 1.23456789E7\n" +
                "value5: 0.001\n" +
                "value6: 6.0\n" +
                "value7: 1.0E12\n" +
                "value8: 8888.8888\n";
        final KafkaEvent event = KafkaBenchmarkDriver.mapper.readValue(str, KafkaEvent.class);
        String str1 = KafkaBenchmarkDriver.mapper.writeValueAsString(event);
        assertEquals(str, str1);
        final byte[] bytes = KafkaBenchmarkDriver.eventToBytes(event);
        final KafkaEvent event2 = KafkaBenchmarkDriver.bytesToEvent(bytes);
        String str2 = KafkaBenchmarkDriver.mapper.writeValueAsString(event2);
        assertEquals(str, str2);
    }
}