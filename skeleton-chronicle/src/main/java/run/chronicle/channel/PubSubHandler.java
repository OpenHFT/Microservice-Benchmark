package run.chronicle.channel;


import net.openhft.affinity.AffinityLock;
import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.time.SystemTimeProvider;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.threads.Pauser;
import net.openhft.chronicle.wire.DocumentContext;
import net.openhft.chronicle.wire.NanoTimestampLongConverter;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import run.chronicle.channel.api.*;
import run.chronicle.channel.impl.BufferedChronicleChannel;
import run.chronicle.channel.impl.ClosedIORuntimeException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PubSubHandler extends SelfDescribingMarshallable implements ChannelHandler {
    private final Map<String, Subscription> subscriptionMap = new LinkedHashMap<>();
    private final List<Subscription> prioritySubscriptions = new ArrayList<>();
    private final List<Subscription> subscriptions = new ArrayList<>();
    private SystemContext systemContext = SystemContext.INSTANCE;
    private String connectionId;
    private boolean buffered;

    public PubSubHandler() {
        this(NanoTimestampLongConverter.INSTANCE.asString(
                SystemTimeProvider.CLOCK.currentTimeNanos()));
    }

    public PubSubHandler(String connectionId) {
        this.connectionId = connectionId;
    }

    @Override
    public SystemContext systemContext() {
        return systemContext;
    }

    @Override
    public String connectionId() {
        return connectionId;
    }

    public PubSubHandler connectionId(String connectionId) {
        this.connectionId = connectionId;
        return this;
    }

    @Override
    public void run(ChronicleContext context, ChronicleChannel channel) {
        Pauser pauser = Pauser.balanced();

        final String id = channel.headerIn().connectionId();
        if (channel instanceof BufferedChronicleChannel) {
            BufferedChronicleChannel bc = (BufferedChronicleChannel) channel;
            bc.eventPoller(this::pollSubscriptions);
        } else {
            Thread tailerThread = new Thread(() -> queueTailer(id, pauser, channel), connectionId + "~tailers");
            tailerThread.setDaemon(true);
            tailerThread.start();
        }

        Thread.currentThread().setName(connectionId + "~reader");
        Map<String, Publication> publicationMap = new LinkedHashMap();
        try (AffinityLock lock = AffinityLock.acquireLock()) {

            while (!channel.isClosed()) {
                try (DocumentContext dc = channel.readingDocument()) {
                    pauser.unpause();

                    if (!dc.isPresent()) {
                        continue;
                    }
                    if (dc.isMetaData()) {
                        // read message
                        continue;
                    }

                    final String s = dc.wire().readEvent(String.class);
                    switch (s) {
                        case "subscribe":
                            final Subscribe subscribe = dc.wire().getValueIn().object(Subscribe.class);
                            addSubscription(subscribe);
                            break;

                        case "unsubscribe":
                            final String unsubscribe = dc.wire().getValueIn().text();
                            removeSubscription(unsubscribe);
                            break;

                        default:
                            String qName = dc.wire().getValueIn().text();
                            if (qName == null || qName.isEmpty())
                                qName = s;

                            Publication pub = publicationMap.get(qName);
                            if (pub == null) {
                                pub = new Publication();
                                pub.queue = newQueue(qName);
                                pub.appender = pub.queue.acquireAppender();
                                publicationMap.put(qName, pub);
                            }
                            try (DocumentContext dc2 = pub.appender.writingDocument()) {
                                dc.wire().copyTo(dc2.wire());
                            }
                    }

                } catch (ClosedIORuntimeException e) {
                    if (!channel.isClosed())
                        Jvm.warn().on(getClass(), e);
                    break;
                }
            }

        } catch (ClosedIORuntimeException e) {
            Jvm.warn().on(getClass(), e.toString());

        } finally {
            Closeable.closeQuietly(publicationMap.values());
            synchronized (subscriptionMap) {
                Closeable.closeQuietly(subscriptionMap.values());
            }
            Thread.currentThread().setName("connections");
        }
    }

    private void removeSubscription(String unsubscribe) {
        synchronized (subscriptionMap) {
            final Subscription removed = subscriptionMap.remove(unsubscribe);
            if (removed != null)
                Closeable.closeQuietly(removed.tailer, removed.queue);
            updateSubscriptionLists();
        }
    }

    void addSubscription(Subscribe subscribe) {
        synchronized (subscriptionMap) {
            final String name = subscribe.name();
            Subscription subscription = subscriptionMap.get(name);
            if (subscription == null) {
                subscription = new Subscription();
                subscriptionMap.put(name, subscription);
                subscription.name(name);
                subscription.queue = newQueue(name);
                subscription.tailer = subscription.queue.createTailer(connectionId());
            }
            subscription.eventType(subscribe.eventType());
            subscription.priority(subscribe.priority());

            updateSubscriptionLists();
        }
    }

    private void updateSubscriptionLists() {
        assert Thread.holdsLock(subscriptionMap);
        subscriptions.clear();
        prioritySubscriptions.clear();
        for (Subscription sub : subscriptionMap.values()) {
            (sub.priority() ? prioritySubscriptions : subscriptions).add(sub);
        }
    }

    private void queueTailer(String id, Pauser pauser, ChronicleChannel channel) {
        try (AffinityLock lock = AffinityLock.acquireLock()) {
            while (!channel.isClosed()) {
                if (pollSubscriptions(channel))
                    pauser.reset();
                else
                    pauser.pause();
            }
        }
    }

    private boolean pollSubscriptions(ChronicleChannel conn) {
        synchronized (subscriptionMap) {
            boolean wrote = false;
            for (int i = 0; i < prioritySubscriptions.size(); i++)
                while (copyOneMessage(conn, prioritySubscriptions.get(i)))
                    wrote = true;
            if (wrote)
                return true;
            for (int i = 0; i < subscriptions.size(); i++)
                while (copyOneMessage(conn, subscriptions.get(i)))
                    wrote = true;
            return wrote;
        }
    }

    private boolean copyOneMessage(ChronicleChannel channel, Subscription subscription) {
        try (DocumentContext dc = subscription.tailer.readingDocument()) {
            if (!dc.isPresent()) {
                return false;
            }
            if (dc.isMetaData()) {
                return false;
            }

            final long dataBuffered;
            try (DocumentContext dc2 = channel.writingDocument()) {
                dc2.wire().write(subscription.eventType()).text(subscription.name());
                dc.wire().copyTo(dc2.wire());

                dataBuffered = dc2.wire().bytes().writePosition();
            }
            // wait for it to drain
            return dataBuffered < 32 << 10;
        }
    }

    private ChronicleQueue newQueue(String subscribe) {
        return ChronicleQueue.singleBuilder(subscribe).blockSize(OS.isSparseFileSupported() ? 512L << 30 : 64L << 20).build();
    }

    static class Publication implements Closeable {
        public ChronicleQueue queue;
        public ExcerptAppender appender;

        @Override
        public void close() {
            appender.close();
            queue.close();
        }

        @Override
        public boolean isClosed() {
            return queue.isClosed();
        }
    }

    static class Subscription extends Subscribe implements Closeable {
        ChronicleQueue queue;
        ExcerptTailer tailer;

        @Override
        public void close() {
            tailer.close();
            queue.close();
        }

        @Override
        public boolean isClosed() {
            return queue.isClosed();
        }
    }
}