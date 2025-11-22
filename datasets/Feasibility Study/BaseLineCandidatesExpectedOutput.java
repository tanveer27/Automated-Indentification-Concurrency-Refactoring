// ==========================
// File: java/refactor_solutions/OptimizedSamples.java
// ==========================

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;

// Note: These show common developer patterns seen in practice for concurrency and parallelism.
public class OptimizedSamples {

    // [CAT: streams] IntStream with parallel. Often faster for large ranges.
    public static long sumRangeParallel(int n) {
        return LongStream.range(0, n).parallel().sum();
    }

    // [CAT: streams+io] parallel lines count with try-with-resources per element.
    public static long countLinesInFilesParallel(List<File> files) {
        return files.parallelStream().mapToLong(f -> {
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                return br.lines().count();
            } catch (IOException e) {
                return 0L;
            }
        }).sum();
    }

    // [CAT: streams] parallel filter.
    public static List<Integer> filterEvensParallel(List<Integer> numbers) {
        return numbers.parallelStream().filter(n -> n % 2 == 0).collect(Collectors.toList());
    }

    // [CAT: parallel reduction] factorial via rangeClosed reduce. For large n use BigInteger.
    public static long factorialParallel(int n) {
        return LongStream.rangeClosed(1, n).parallel().reduce(1L, (a, b) -> a * b);
    }

    // [CAT: arrays] parallel sort.
    public static void parallelSort(int[] arr) { Arrays.parallelSort(arr); }

    // [CAT: collectors] word count using groupingByConcurrent.
    public static Map<String, Long> wordCountParallel(List<String> words) {
        return words.parallelStream().collect(Collectors.groupingByConcurrent(w -> w, Collectors.counting()));
    }

    // [CAT: forkjoin] sum of squares using ForkJoin RecursiveTask.
    static class SquaresSumTask extends RecursiveTask<Long> {
        private static final int THRESHOLD = 10_000;
        private final int[] data; private final int start; private final int end;
        SquaresSumTask(int[] data, int start, int end) { this.data = data; this.start = start; this.end = end; }
        @Override protected Long compute() {
            int len = end - start;
            if (len <= THRESHOLD) {
                long s = 0L; for (int i = start; i < end; i++) s += (long) data[i] * data[i];
                return s;
            }
            int mid = start + len / 2;
            SquaresSumTask left = new SquaresSumTask(data, start, mid);
            SquaresSumTask right = new SquaresSumTask(data, mid, end);
            left.fork();
            long r = right.compute();
            long l = left.join();
            return l + r;
        }
    }
    public static long computeSquaresForkJoin(int[] data) {
        return ForkJoinPool.commonPool().invoke(new SquaresSumTask(data, 0, data.length));
    }

    // [CAT: executor] fixed thread pool to fetch URLs concurrently. Bounded parallelism.
    public static List<String> fetchUrlsWithExecutor(List<String> urls, int threads) throws InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(threads);
        try {
            List<Callable<String>> tasks = new ArrayList<>();
            for (String u : urls) {
                tasks.add(() -> {
                    try {
                        URL url = new URL(u);
                        try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
                            StringBuilder sb = new StringBuilder(); String line;
                            while ((line = br.readLine()) != null) sb.append(line).append('\n');
                            return sb.toString();
                        }
                    } catch (IOException e) { return ""; }
                });
            }
            List<Future<String>> fs = es.invokeAll(tasks);
            List<String> out = new ArrayList<>(fs.size());
            for (Future<String> f : fs) try { out.add(f.get()); } catch (ExecutionException e) { out.add(""); }
            return out;
        } finally { es.shutdown(); }
    }

    // [CAT: completablefuture] async fetch with custom pool and allOf join.
    public static List<String> fetchUrlsWithCF(List<String> urls, int threads) {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<CompletableFuture<String>> cfs = urls.stream().map(u ->
                    CompletableFuture.supplyAsync(() -> {
                        try {
                            URL url = new URL(u);
                            try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
                                StringBuilder sb = new StringBuilder(); String line;
                                while ((line = br.readLine()) != null) sb.append(line).append('\n');
                                return sb.toString();
                            }
                        } catch (IOException e) { return ""; }
                    }, pool)
            ).toList();
            CompletableFuture.allOf(cfs.toArray(new CompletableFuture[0])).join();
            return cfs.stream().map(CompletableFuture::join).toList();
        } finally { pool.shutdown(); }
    }

    // [CAT: arrays] parallel prefix sum.
    public static void prefixSumParallel(int[] a) { Arrays.parallelPrefix(a, Integer::sum); }

    // [CAT: files.walk] parallel size sum using parallel stream over walk.
    public static long totalSizeParallel(Path root) {
        try (var s = Files.walk(root)) {
            return s.parallel().filter(Files::isRegularFile).mapToLong(p -> {
                try { return Files.size(p); } catch (IOException e) { return 0L; }
            }).sum();
        } catch (IOException e) { return 0L; }
    }

    // [CAT: accumulators] LongAdder for high-contention counters.
    public static long countMatchesWithLongAdder(List<String> data, String needle) {
        LongAdder adder = new LongAdder();
        data.parallelStream().forEach(s -> { if (s.contains(needle)) adder.increment(); });
        return adder.sum();
    }

    // [CAT: concurrentmap] frequency via ConcurrentHashMap.compute.
    public static Map<String, Long> wordCountConcurrentMap(List<String> words) {
        ConcurrentHashMap<String, Long> m = new ConcurrentHashMap<>();
        words.parallelStream().forEach(w -> m.compute(w, (k, v) -> v == null ? 1L : v + 1));
        return m;
    }

    // [CAT: rate-limit] semaphore to limit concurrency while processing.
    public static List<String> processWithSemaphore(List<String> items, int maxConcurrent) {
        Semaphore sem = new Semaphore(maxConcurrent);
        return items.parallelStream().map(it -> {
            try {
                sem.acquire();
                // simulate work
                return it.toUpperCase();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return it;
            } finally { sem.release(); }
        }).toList();
    }

    // [CAT: coordination] CountDownLatch to await N tasks.
    public static int runTasksWithLatch(int tasks) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(tasks);
        ExecutorService es = Executors.newCachedThreadPool();
        AtomicInteger done = new AtomicInteger();
        for (int i = 0; i < tasks; i++) {
            es.submit(() -> { try { /* work */ done.incrementAndGet(); } finally { latch.countDown(); } });
        }
        latch.await();
        es.shutdown();
        return done.get();
    }

    // [CAT: phaser] phased processing in waves.
    public static int phasedBatches(int participants, int phases) {
        Phaser ph = new Phaser(participants);
        ExecutorService es = Executors.newFixedThreadPool(participants);
        AtomicInteger ops = new AtomicInteger();
        for (int i = 0; i < participants; i++) {
            es.submit(() -> {
                for (int p = 0; p < phases; p++) {
                    // phase work
                    ops.incrementAndGet();
                    ph.arriveAndAwaitAdvance();
                }
            });
        }
        es.shutdown();
        try { es.awaitTermination(1, TimeUnit.MINUTES); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        return ops.get();
    }
}