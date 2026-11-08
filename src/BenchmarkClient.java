
import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class BenchmarkClient {

    static final int CLIENTS = 8;
    static final int REQUESTS_PER_CLIENT = 100;
    static final int PORT = 6379;

    public static void main(String[] args)
            throws Exception {

        ExecutorService pool =
            Executors.newFixedThreadPool(CLIENTS);

        CountDownLatch ready =
            new CountDownLatch(CLIENTS);

        CountDownLatch start =
            new CountDownLatch(1);

        List<Future<List<Long>>> futures =
            new ArrayList<>();

        for (int i = 0; i < CLIENTS; i++) {

            futures.add(pool.submit(() -> {

                List<Long> latencies = new ArrayList<>();

                try (Socket socket =
                         new Socket("127.0.0.1", PORT);

                     BufferedReader reader =
                         new BufferedReader(
                             new InputStreamReader(
                                 socket.getInputStream(),
                                 StandardCharsets.UTF_8
                             )
                         );

                     PrintWriter writer =
                         new PrintWriter(
                             new OutputStreamWriter(
                                 socket.getOutputStream(),
                                 StandardCharsets.UTF_8
                             ), true
                         )) {

                    socket.setSoTimeout(10000);

                    ready.countDown();
                    start.await();

                    for (int j = 0;
                         j < REQUESTS_PER_CLIENT;
                         j++) {

                        long begin = System.nanoTime();

                        writer.println("GET benchmark_key");
                        String response = reader.readLine();

                        long end = System.nanoTime();

                        if (!"VALUE benchmark_value".equals(response)) {
                            throw new IllegalStateException(
                                "Unexpected response: " + response
                            );
                        }

                        latencies.add(end - begin);
                    }
                }

                return latencies;
            }));
        }

        ready.await();
        long benchmarkStart = System.nanoTime();
        start.countDown();

        List<Long> all = new ArrayList<>();

        for (Future<List<Long>> future : futures) {
            all.addAll(future.get());
        }

        long benchmarkEnd = System.nanoTime();
        pool.shutdown();

        Collections.sort(all);

        double seconds =
            (benchmarkEnd - benchmarkStart) / 1e9;

        int total = all.size();

        System.out.printf(
            "Throughput: %.2f requests/sec%n",
            total / seconds
        );

        System.out.printf(
            "p50: %.3f ms%n",
            percentile(all, 50) / 1e6
        );

        System.out.printf(
            "p95: %.3f ms%n",
            percentile(all, 95) / 1e6
        );

        System.out.printf(
            "p99: %.3f ms%n",
            percentile(all, 99) / 1e6
        );
    }

    static long percentile(List<Long> sorted, int p) {
        int index = (int) Math.ceil(
            p / 100.0 * sorted.size()
        ) - 1;

        return sorted.get(Math.max(0, index));
    }
}

