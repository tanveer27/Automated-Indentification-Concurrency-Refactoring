// ==========================
// File: java/refactor_baseline/BaselineCandidates.java
// ==========================

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

public class BaselineCandidates {

    // [CAT: loops] simple loop summation. Stream or parallel replacement possible.
    public static long sumRangeLoop(int n) {
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += i;
        }
        return sum;
    }

    // [CAT: io] sequential line counting across files. Parallelization possible.
    public static long countLinesInFiles(List<File> files) throws IOException {
        long total = 0;
        for (File f : files) {
            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                while (br.readLine() != null) total++;
            }
        }
        return total;
    }

    // [CAT: loops] filter evens using external iteration.
    public static List<Integer> filterEvens(List<Integer> numbers) {
        List<Integer> result = new ArrayList<>();
        for (Integer n : numbers) {
            if (n % 2 == 0) result.add(n);
        }
        return result;
    }

    // [CAT: recursion] naive recursive factorial.
    public static long factorialRecursive(int n) {
        if (n <= 1) return 1;
        return n * factorialRecursive(n - 1);
    }

    // [CAT: sorting] manual quadratic sort.
    public static void manualSort(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            for (int j = i + 1; j < arr.length; j++) {
                if (arr[i] > arr[j]) {
                    int t = arr[i]; arr[i] = arr[j]; arr[j] = t;
                }
            }
        }
    }

    // [CAT: aggregation] word frequency with plain HashMap and sequential loop.
    public static Map<String, Long> wordCount(List<String> words) {
        Map<String, Long> freq = new HashMap<>();
        for (String w : words) {
            freq.put(w, freq.getOrDefault(w, 0L) + 1);
        }
        return freq;
    }

    // [CAT: arithmetic] sum of squares, sequential.
    public static long computeSquaresSum(List<Integer> numbers) {
        long sum = 0;
        for (Integer n : numbers) sum += (long) n * n;
        return sum;
    }

    // [CAT: io] classic copy loop.
    public static void copyFile(File src, File dest) throws IOException {
        try (InputStream in = new FileInputStream(src);
             OutputStream out = new FileOutputStream(dest)) {
            byte[] buf = new byte[8192];
            int len;
            while ((len = in.read(buf)) > 0) out.write(buf, 0, len);
        }
    }

    // [CAT: primality] sequential prime count.
    public static long countPrimes(int limit) {
        long c = 0;
        for (int i = 2; i < limit; i++) if (isPrime(i)) c++;
        return c;
    }
    private static boolean isPrime(int n) {
        if (n < 2) return false;
        for (int d = 2; d * d <= n; d++) if (n % d == 0) return false;
        return true;
    }

    // [CAT: averages] sequential average.
    public static double averageList(List<Double> list) {
        double s = 0.0;
        for (double d : list) s += d;
        return list.isEmpty() ? 0.0 : s / list.size();
    }

    // [CAT: recursion] naive Fibonacci. Parallelization candidates exist.
    public static long fibRecursive(int n) {
        if (n <= 1) return n;
        return fibRecursive(n - 1) + fibRecursive(n - 2);
    }

    // [CAT: io+net] sequential URL fetch. Executor or CF can improve.
    public static List<String> fetchUrlsSequential(List<String> urls) {
        List<String> bodies = new ArrayList<>();
        for (String u : urls) {
            try {
                URL url = new URL(u);
                try (BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream()))) {
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = br.readLine()) != null) sb.append(line).append('\n');
                    bodies.add(sb.toString());
                }
            } catch (IOException e) {
                bodies.add("");
            }
        }
        return bodies;
    }

    // [CAT: arrays] manual prefix sum. Arrays.parallelPrefix can help.
    public static void prefixSum(int[] a) {
        for (int i = 1; i < a.length; i++) a[i] += a[i - 1];
    }

    // [CAT: walk] sequential file size sum using Files.walk but single thread.
    public static long totalSizeSequential(Path root) {
        try {
            long sum = 0L;
            try (var s = Files.walk(root)) {
                for (Path p : (Iterable<Path>) s::iterator) {
                    if (Files.isRegularFile(p)) sum += Files.size(p);
                }
            }
            return sum;
        } catch (IOException e) {
            return 0L;
        }
    }
}
