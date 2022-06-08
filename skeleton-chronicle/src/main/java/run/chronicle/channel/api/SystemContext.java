package run.chronicle.channel.api;

import net.openhft.chronicle.bytes.DistributedUniqueTimeProvider;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.core.time.TimeProvider;
import net.openhft.chronicle.wire.LongConversion;
import net.openhft.chronicle.wire.NanoTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

@SuppressWarnings("unused")
public class SystemContext extends SelfDescribingMarshallable {
    public static final SystemContext INSTANCE = getInstance();
    private int availableProcessors;
    private String chronicleBomVersion;
    private String hostId;
    private String hostName;
    private String javaVendor;
    private String javaVersion;
    @LongConversion(NanoTimestampLongConverter.class)
    private long localTime;
    private int maxMemoryMB;
    private String osArch;
    private String userCountry;
    private String userName;

    private static SystemContext getInstance() {
        SystemContext sc = new SystemContext();
        final Runtime runtime = Runtime.getRuntime();
        sc.availableProcessors = runtime.availableProcessors();
        sc.chronicleBomVersion = "2.23ea-SNAPSHOT";
        sc.hostId = System.getProperty("hostId");
        sc.hostName = OS.getHostName();
        sc.javaVendor = System.getProperty("java.vendor");
        sc.javaVersion = System.getProperty("java.version");
        final TimeProvider tp = (sc.hostId == null ? SystemTimeProvider.INSTANCE : DistributedUniqueTimeProvider.instance());
        sc.localTime = tp.currentTimeNanos();
        sc.maxMemoryMB = (int) (runtime.maxMemory() >> 20);
        sc.osArch = System.getProperty("os.arch");
        sc.userCountry = System.getProperty("user.country");
        sc.userName = OS.getUserName();
        return sc;
    }

    public static void main(String[] args) {
        System.out.println(getInstance());
    }
}
