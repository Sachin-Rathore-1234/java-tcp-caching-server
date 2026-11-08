
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class ConcurrentLRUCache implements AutoCloseable {

    private static final Logger logger =
        Logger.getLogger(ConcurrentLRUCache.class.getName());

    private static class Node {
        String key, value;
        long expiresAt;
        boolean neverExpires;
        Node prev, next;

        Node(String key, String value,
             long expiresAt, boolean neverExpires) {
            this.key = key;
            this.value = value;
            this.expiresAt = expiresAt;
            this.neverExpires = neverExpires;
        }
    }

    private final int capacity;
    private final Map<String, Node> map = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final Node head = new Node(null, null, 0, true);
    private final Node tail = new Node(null, null, 0, true);

    private final ScheduledExecutorService cleaner;

    private long hits = 0;
    private long misses = 0;
    private long evictions = 0;
    private long expirations = 0;

    public ConcurrentLRUCache(int capacity) {
        if (capacity <= 0)
            throw new IllegalArgumentException("Invalid capacity");

        this.capacity = capacity;
        head.next = tail;
        tail.prev = head;

        cleaner = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "ttl-cleaner");
            t.setDaemon(true);
            return t;
        });

        cleaner.scheduleWithFixedDelay(
            this::cleanupExpired,
            5, 5, TimeUnit.SECONDS
        );
    }

    private void addFirst(Node node) {
        node.next = head.next;
        node.prev = head;
        head.next.prev = node;
        head.next = node;
    }

    private void remove(Node node) {
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev = null;
        node.next = null;
    }

    private void moveToFront(Node node) {
        remove(node);
        addFirst(node);
    }

    private boolean isExpired(Node node) {
        return !node.neverExpires &&
               System.nanoTime() - node.expiresAt >= 0;
    }

    private void removeExpired(Node node) {
        remove(node);
        map.remove(node.key);
        expirations++;
    }

    public String get(String key) {
        lock.lock();
        try {
            Node node = map.get(key);

            if (node == null) {
                misses++;
                return null;
            }

            if (isExpired(node)) {
                removeExpired(node);
                misses++;
                return null;
            }

            moveToFront(node);
            hits++;
            return node.value;
        } finally {
            lock.unlock();
        }
    }

    public void put(String key, String value, long ttlMs) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);

        if (ttlMs < 0 ||
            ttlMs > TimeUnit.DAYS.toMillis(365)) {
            throw new IllegalArgumentException("Invalid TTL");
        }

        boolean neverExpires = ttlMs == 0;
        long expiresAt = neverExpires ? 0 :
            System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(ttlMs);

        lock.lock();
        try {
            Node existing = map.get(key);

            if (existing != null) {
                existing.value = value;
                existing.expiresAt = expiresAt;
                existing.neverExpires = neverExpires;
                moveToFront(existing);
                return;
            }

            if (map.size() == capacity) {
                Node lru = tail.prev;
                remove(lru);
                map.remove(lru.key);
                evictions++;
            }

            Node node = new Node(
                key, value, expiresAt, neverExpires
            );

            addFirst(node);
            map.put(key, node);
        } finally {
            lock.unlock();
        }
    }

    public boolean delete(String key) {
        lock.lock();
        try {
            Node node = map.remove(key);

            if (node == null)
                return false;

            remove(node);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void cleanupExpired() {
        lock.lock();
        try {
            Node curr = head.next;

            while (curr != tail) {
                Node next = curr.next;

                if (isExpired(curr)) {
                    removeExpired(curr);
                }

                curr = next;
            }

            logger.info("TTL cleanup completed");
        } finally {
            lock.unlock();
        }
    }

    public String stats() {
        lock.lock();
        try {
            long total = hits + misses;
            double hitRate = total == 0 ? 0 :
                100.0 * hits / total;

            return String.format(
                "size=%d hits=%d misses=%d " +
                "hitRate=%.2f%% evictions=%d expirations=%d",
                map.size(), hits, misses,
                hitRate, evictions, expirations
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        cleaner.shutdownNow();
    }
}
