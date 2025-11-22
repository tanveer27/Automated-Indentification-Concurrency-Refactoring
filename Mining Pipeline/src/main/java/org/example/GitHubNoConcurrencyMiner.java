package org.example;


import org.kohsuke.github.*;
import org.apache.commons.csv.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * GitHubNoConcurrencyMiner (weighted)
 *
 * Finds Java repos with ZERO concurrency/parallelism usage and passes a weighted maturity filter.
 *
 * Output CSV columns (exactly as requested):
 *   Project, URL, Commits, Stars, PRs, Issues
 *
 * Tweakable knobs:
 *   - MIN_* thresholds for scoring
 *   - W_* weights
 *   - PASS_SCORE
 *   - TARGET_COUNT (how many clean repos to output)
 *
 *   Dataset B - part 1
 */
public class GitHubNoConcurrencyMiner {

    // ---------- Target & seed ----------
    private static final int TARGET_COUNT = 10;
    private static final String PUSHED_SINCE = "2020-01-01"; // seed recency
    private static final String OUTPUT_CSV = "phase3_strict_clean_repos.csv";

    // ---------- Weighted maturity thresholds ----------
    private static final int MIN_COMMITS   = 150;
    private static final int MIN_PRS       = 10;   // all states
    private static final int MIN_ISSUES    = 10;   // all states
    private static final int MIN_CONTRIBS  = 2;

    private static final double W_COMMITS  = 0.40;
    private static final double W_PRS      = 0.25;
    private static final double W_ISSUES   = 0.20;
    private static final double W_CONTRIBS = 0.15;

    private static final double PASS_SCORE = 0.75;

    // ---------- Forbidden constructs to exclude (concurrency/parallelism) ----------
    private static final String[] FORBIDDEN = {
            // Parallel streams
            ".parallel()", "parallelStream(",
            // Fork/Join
            "ForkJoinPool", "ForkJoinTask", "RecursiveTask", "RecursiveAction",
            // Executors / threads
            "Executor ", "Executor<", "ExecutorService", "Executors.", "ThreadPoolExecutor",
            "new Thread(", "Thread.sleep(",
            // Futures / CompletableFuture
            "Future<", "CompletionStage", "CompletableFuture",
            "supplyAsync(", "runAsync(", "thenApply(", "thenApplyAsync(", "thenCompose(", "thenComposeAsync(",
            "thenAccept(", "thenAcceptAsync(", "allOf(", "anyOf(",
            // Spring async
            "ListenableFuture", "ListenableFutureCallback", "addCallback("
    };

    public static void main(String[] args) throws Exception {
        String token = System.getenv("GITHUB_TOKEN");
        GitHub gh = (token == null || token.isBlank())
                ? GitHub.connectAnonymously()
                : new GitHubBuilder().withOAuthToken(token).build();

        GHRateLimit rateLimit = gh.getRateLimit();
        System.out.println("Rate limit: limit=" + rateLimit.getLimit() + " remaining=" + rateLimit.getRemaining());

        // Broad Java seed;
        PagedSearchIterable<GHRepository> repos = gh.searchRepositories()
                .q("language:Java pushed:>=" + PUSHED_SINCE + " fork:false archived:false")
                .sort(GHRepositorySearchBuilder.Sort.STARS)
                .order(GHDirection.DESC)
                .list();

        int found = 0;
        try (CSVPrinter out = new CSVPrinter(new FileWriter(OUTPUT_CSV, StandardCharsets.UTF_8),
                CSVFormat.DEFAULT.withHeader("Project","URL","Commits","Stars","PRs","Issues"))) {

            for (GHRepository r : repos) {
                if (found >= TARGET_COUNT) break;

                String slug = r.getFullName();
                System.out.println("Candidate: " + slug);

                // ---- 1) Weighted maturity scoring (fast early-exit counters) ----
                boolean mCommits  = hasCommitsAtLeast(r, MIN_COMMITS);
                boolean mPRs      = hasPRsAtLeast(gh, slug, MIN_PRS);
                boolean mIssues   = hasIssuesAtLeast(gh, slug, MIN_ISSUES);
                boolean mContribs = hasContributorsAtLeast(r, MIN_CONTRIBS);

                double score = (mCommits ? W_COMMITS : 0)
                        + (mPRs ? W_PRS : 0)
                        + (mIssues ? W_ISSUES : 0)
                        + (mContribs ? W_CONTRIBS : 0);

                if (score < PASS_SCORE) {
                    continue; // not mature enough
                }

                // ---- 2) Forbidden token prefilter (code search) ----
                if (hasForbiddenByCodeSearch(gh, slug)) {
                    System.out.println("  Rejected by code search.");
                    continue;
                }

                // ---- 3) Zip verification (local scan of .java files) ----
                if (!passesZipVerification(slug, token)) {
                    System.out.println("  Rejected by zip verification.");
                    continue;
                }

                // ---- 4) Compute actual counts for CSV (only for accepted repos) ----
                int commitCount = countAllCommits(r, 50000); // generous cap
                int prCount     = countAllPRs(gh, slug, 50000);
                int issueCount  = countAllIssues(gh, slug, 50000);
                int stars       = r.getStargazersCount();

                out.printRecord(slug, r.getHtmlUrl().toString(), commitCount, stars, prCount, issueCount);
                out.flush();
                found++;

                // polite throttle
                Thread.sleep(200);
            }
        }

        System.out.println("Done. Clean repos written to: " + OUTPUT_CSV + " (count=" + found + ")");
    }

    // -------------------- maturity helpers (early-exit) --------------------

    private static boolean hasContributorsAtLeast(GHRepository repo, int threshold) throws IOException {
        int count = 0;
        for (GHRepository.Contributor c : repo.listContributors().withPageSize(100)) {
            count++;
            if (count >= threshold) return true;
        }
        return false;
    }

    private static boolean hasCommitsAtLeast(GHRepository repo, int threshold) throws IOException {
        int count = 0;
        for (GHCommit c : repo.listCommits().withPageSize(100)) {
            count++;
            if (count >= threshold) return true;
        }
        return false;
    }

    private static boolean hasPRsAtLeast(GitHub gh, String slug, int threshold) throws IOException {
        int count = 0;
        PagedSearchIterable<GHIssue> prs = gh.searchIssues().q("repo:" + slug + " is:pr").list();
        for (GHIssue ignored : prs.withPageSize(100)) {
            count++;
            if (count >= threshold) return true;
        }
        return false;
    }

    private static boolean hasIssuesAtLeast(GitHub gh, String slug, int threshold) throws IOException {
        int count = 0;
        PagedSearchIterable<GHIssue> issues = gh.searchIssues().q("repo:" + slug + " is:issue").list();
        for (GHIssue ignored : issues.withPageSize(100)) {
            count++;
            if (count >= threshold) return true;
        }
        return false;
    }

    // -------------------- forbidden detection --------------------

    private static boolean hasForbiddenByCodeSearch(GitHub gh, String slug) throws IOException, InterruptedException {
        int checked = 0;
        for (String tok : FORBIDDEN) {
            String q = "repo:" + slug + " " + normalizeTokenForSearch(tok);
            PagedSearchIterable<GHContent> results = gh.searchContent().q(q).list();
            if (results.iterator().hasNext()) {
                return true;
            }
            checked++;
            if (checked % 5 == 0) Thread.sleep(150);
        }
        return false;
    }

    private static String normalizeTokenForSearch(String tok) {
        String t = tok.replace("(", " ").replace(")", " ").trim();
        if (t.isEmpty()) t = tok;
        if (t.contains(".") || t.contains("<") || t.contains(">")) {
            return "\"" + t.replace("\"","") + "\"";
        }
        return t;
    }

    // -------------------- zip verification --------------------

    private static boolean passesZipVerification(String slug, String token) {
        Path tmp = null;
        try {
            tmp = Files.createTempDirectory("noconc_" + slug.replace('/','_') + "_");
            Path root = tmp.resolve("repo");
            Files.createDirectories(root);
            downloadRepoZip(slug, root, token);

            List<Path> javaFiles = listJavaFiles(root);
            for (Path f : javaFiles) {
                List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
                for (String raw : lines) {
                    String L = stripLineComments(stripStringLiterals(raw));
                    for (String tok : FORBIDDEN) {
                        if (L.contains(tok)) {
                            return false; // forbidden found
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            System.err.println("  Zip verify error for " + slug + ": " + e.getMessage());
            return false;
        } finally {
            deleteQuietly(tmp);
        }
    }

    // -------------------- counts for accepted repos --------------------

    private static int countAllCommits(GHRepository repo, int cap) throws IOException {
        int count = 0;
        for (GHCommit c : repo.listCommits().withPageSize(100)) {
            count++;
            if (count >= cap) return cap;
        }
        return count;
    }

    private static int countAllPRs(GitHub gh, String slug, int cap) throws IOException {
        int count = 0;
        PagedSearchIterable<GHIssue> prs = gh.searchIssues().q("repo:" + slug + " is:pr").list();
        for (GHIssue ignored : prs.withPageSize(100)) {
            count++;
            if (count >= cap) return cap;
        }
        return count;
    }

    private static int countAllIssues(GitHub gh, String slug, int cap) throws IOException {
        int count = 0;
        PagedSearchIterable<GHIssue> issues = gh.searchIssues().q("repo:" + slug + " is:issue").list();
        for (GHIssue ignored : issues.withPageSize(100)) {
            count++;
            if (count >= cap) return cap;
        }
        return count;
    }

    // -------------------- zip utils --------------------

    private static void downloadRepoZip(String slug, Path destRoot, String token) throws IOException {
        String zipUrl = "https://api.github.com/repos/" + slug + "/zipball";
        HttpURLConnection conn = (HttpURLConnection) new URL(zipUrl).openConnection();
        if (token != null && !token.isBlank()) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
        conn.setRequestProperty("Accept", "application/vnd.github+json");
        conn.setInstanceFollowRedirects(true);
        conn.connect();
        try (InputStream in = conn.getInputStream(); ZipInputStream zis = new ZipInputStream(in)) {
            unzipStrippingTopDir(zis, destRoot);
        }
    }

    private static void unzipStrippingTopDir(ZipInputStream zis, Path targetDir) throws IOException {
        ZipEntry entry;
        while ((entry = zis.getNextEntry()) != null) {
            String name = entry.getName();
            int slash = name.indexOf('/');
            String relative = (slash >= 0) ? name.substring(slash + 1) : name;
            if (relative.isEmpty()) continue;

            Path outPath = targetDir.resolve(relative).normalize();
            if (!outPath.startsWith(targetDir)) throw new IOException("Zip entry escapes target dir: " + relative);
            if (entry.isDirectory()) {
                Files.createDirectories(outPath);
            } else {
                Files.createDirectories(outPath.getParent());
                try (OutputStream os = Files.newOutputStream(outPath)) {
                    zis.transferTo(os);
                }
            }
        }
    }

    private static List<Path> listJavaFiles(Path root) throws IOException {
        List<Path> res = new ArrayList<>();
        if (root == null) return res;
        Files.walk(root).filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".java")).forEach(res::add);
        return res;
    }

    private static void deleteQuietly(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }

    // -------------------- small scrubbers --------------------

    private static String stripLineComments(String s) {
        int i = s.indexOf("//");
        return (i >= 0) ? s.substring(0, i) : s;
    }

    private static String stripStringLiterals(String s) {
        if (s == null || s.isEmpty()) return s;
        StringBuilder out = new StringBuilder(s.length());
        boolean inDq = false, inSq = false, esc = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (inDq) { if (esc) { esc=false; continue; } if (c=='\\'){esc=true;continue;} if (c=='"'){inDq=false;} continue; }
            if (inSq) { if (esc) { esc=false; continue; } if (c=='\\'){esc=true;continue;} if (c=='\''){inSq=false;} continue; }
            if (c=='"'){ inDq=true; out.append("\"\""); continue; }
            if (c=='\''){ inSq=true; out.append("''"); continue; }
            out.append(c);
        }
        return out.toString();
    }
}

