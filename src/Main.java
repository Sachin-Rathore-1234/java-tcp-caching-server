import java.util.concurrent.*;

public class Main {

    public static void main(String[] args)
            throws InterruptedException {

        try (ConcurrentLRUCache cache =
                 new ConcurrentLRUCache(100)) {

            ExecutorService pool =
                Executors.newFixedThreadPool(8);

            CountDownLatch latch = new CountDownLatch(1000);

            for (int i = 0; i < 1000; i++) {
                final int id = i;

                pool.submit(() -> {
                    try {
                        String key = "user:" + (id % 50);

                        cache.put(key, "value:" + id, 10000);
                        cache.get(key);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();

            System.out.println(cache.stats());

            pool.shutdown();
        }
    }
}
