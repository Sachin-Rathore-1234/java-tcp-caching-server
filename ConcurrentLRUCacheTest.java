
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConcurrentLRUCacheTest {

    @Test
    void shouldStoreAndRetrieve() {
        try (ConcurrentLRUCache cache =
                 new ConcurrentLRUCache(2)) {

            cache.put("A", "100", 0);

            assertEquals("100", cache.get("A"));
        }
    }

    @Test
    void shouldEvictLeastRecentlyUsed() {
        try (ConcurrentLRUCache cache =
                 new ConcurrentLRUCache(2)) {

            cache.put("A", "100", 0);
            cache.put("B", "200", 0);

            cache.get("A");

            cache.put("C", "300", 0);

            assertNull(cache.get("B"));
            assertEquals("100", cache.get("A"));
            assertEquals("300", cache.get("C"));
        }
    }

    @Test
    void shouldExpireEntry() throws Exception {
        try (ConcurrentLRUCache cache =
                 new ConcurrentLRUCache(2)) {

            cache.put("A", "100", 50);

            Thread.sleep(150);

            assertNull(cache.get("A"));
        }
    }

    @Test
    void shouldDeleteEntry() {
        try (ConcurrentLRUCache cache =
                 new ConcurrentLRUCache(2)) {

            cache.put("A", "100", 0);

            assertTrue(cache.delete("A"));
            assertNull(cache.get("A"));
            assertFalse(cache.delete("A"));
        }
    }
}
