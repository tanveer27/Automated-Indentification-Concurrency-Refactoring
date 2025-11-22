package org.example;

import org.kohsuke.github.*;
import org.apache.commons.csv.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * GitHub Concurrency Project Miner
 *
 * This program searches GitHub for Java projects that utilize concurrency constructs.
 * It applies various filters to ensure the quality and relevance of the projects,
 * and outputs the results to a CSV file.
 *
 * Requirements:
 * - Set the GITHUB_TOKEN environment variable with a valid GitHub personal access token.
 *
 * Note: This code uses the GitHub API and may be subject to rate limiting.
 * Make sure to handle rate limits appropriately.
 * Task A: Code Search seeding on concurrency signals...
 * Taks B: Repository-level filtering
 * Task C: Strict concurrency verification via repo-scoped search
 * Task D: Metadata extraction and CSV output
 * Task E: Project type classification
 * Task F: Output results to CSV
 * Dataset A generation - Part 1
 */
public class GitHubConcurrencyMiner {

    // ---------- Tuning thresholds ----------
    private static final int TARGET_COUNT    = 100;
    private static final int MIN_STARS       = 0;
    private static final int MIN_COMMITS     = 10;
    private static final int MIN_PRS         = 0;
    private static final int MIN_ISSUES      = 0;
    private static final int MIN_CONTRIBS    = 1;
    private static final int INDUSTRIAL_STARS_THRESHOLD = 500;

    private static final String[] SEED_QUERIES = new String[]{
            "language:Java path:src in:file extension:java \"parallelStream(\"",
            "language:Java path:src in:file extension:java \"java.util.concurrent\"",
            "language:Java path:src in:file extension:java \"java.util.stream\"",
            "language:Java path:src in:file extension:java \"ExecutorService\"",
            "language:Java path:src in:file extension:java \"CompletableFuture\"",
            "language:Java path:src in:file extension:java \"ForkJoinPool\"",
            "language:Java path:src in:file extension:java \"ReentrantLock\"",
            "language:Java path:src in:file extension:java \"Semaphore\""
    };

    private static final String CSV_NAME = "github_concurrency_projects.csv";

    public static void main(String[] args) throws Exception {
        String token = System.getenv("GITHUB_TOKEN");
        if (token == null || token.isEmpty()) {
            System.err.println("ERROR: export GITHUB_TOKEN first (do not hard-code tokens).");
            return;
        }

        GitHub github = new GitHubBuilder().withOAuthToken(token).build();
        RateLimitChecker rate = new RateLimitChecker(github);

        try (FileWriter out = new FileWriter(CSV_NAME);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(
                     "Repository",
                     "URL",
                     "Owner",
                     "Owner Type",
                     "Default Branch",

                     "Stars",
                     "Forks",
                     "Subscribers",
                     "Open Issues (total)",
                     "Commits (total)",
                     "PRs (total)",
                     "Issues (total)",
                     "Contributors (total)",

                     "Created At",
                     "Pushed At",
                     "Updated At",
                     "Last Release Date",

                     "Primary Language",
                     "License",
                     "Topics",
                     "Size (KB)",
                     "Homepage",
                     "Has Wiki",
                     "Has Pages",
                     "Has Build System",
                     "Has CI",

                     "Contains Parallel/Concurrency Code",
                     "Concurrency Signal",
                     "Project Type"
             ))) {

            Set<String> seen = new HashSet<>();
            int added = 0;

            System.out.println("Phase A: Code Search seeding on concurrency signals...");
            outer:
            for (String q : SEED_QUERIES) {
                PagedSearchIterable<GHContent> hits = github.searchContent().q(q).list().withPageSize(50);
                for (GHContent match : hits) {
                    if (added >= TARGET_COUNT) break outer;

                    GHRepository repo = match.getOwner();
                    if (repo == null) continue;
                    String full = repo.getFullName();
                    if (!seen.add(full)) continue;

                    System.out.printf("→ Candidate from [%s]: %s%n", shortQuery(q), full);

                    rate.ensure();
                    if (repo.isFork())      { System.out.println("  • Skip (fork)"); continue; }
                    if (repo.isArchived())  { System.out.println("  • Skip (archived)"); continue; }
                    if (repo.getStargazersCount() < MIN_STARS) {
                        System.out.println("  • Skip (stars < " + MIN_STARS + ")"); continue;
                    }

                    rate.ensure();
                    int prTotal    = fastSearchIssuesTotal(github, "repo:" + full + " is:pr");
                    rate.ensure();
                    int issueTotal = fastSearchIssuesTotal(github, "repo:" + full + " is:issue");

                    if (prTotal < MIN_PRS)         { System.out.println("  • Skip (PRs total = " + prTotal + " < " + MIN_PRS + ")"); continue; }
                    if (issueTotal < MIN_ISSUES)   { System.out.println("  • Skip (Issues total = " + issueTotal + " < " + MIN_ISSUES + ")"); continue; }

                    int commitsTotal = fastCommitCountAll(token, repo);
                    if (commitsTotal < MIN_COMMITS) {
                        System.out.println("  • Skip (Commits total = " + commitsTotal + " < " + MIN_COMMITS + ")"); continue;
                    }

                    int contribs = fastContributorsCount(token, repo);
                    if (contribs < MIN_CONTRIBS) {
                        System.out.println("  • Skip (Contributors total = " + contribs + " < " + MIN_CONTRIBS + ")"); continue;
                    }

                    String desc = safeLower(repo.getDescription());
                    String readmeLower = fetchReadmeLower(repo);
                    if (looksAcademic(desc, readmeLower)) {
                        System.out.println("  • Skip (looks academic/assignment)");
                        continue;
                    }

                    boolean hasBuild = hasBuildSystem(repo);
                    boolean hasCi    = hasCiConfig(repo);
                    if (!hasBuild && !hasCi) {
                        System.out.println("  • Skip (no build system and no CI)");
                        continue;
                    }

                    rate.ensure();
                    String signal = verifyConcurrencyInRepo(github, full);
                    if (signal.isEmpty()) {
                        System.out.println("  • Skip (no verified concurrency import/code via repo-scoped search)");
                        continue;
                    }

                    String ownerLogin = "";
                    String ownerType  = "";
                    try {
                        GHUser owner = repo.getOwner();
                        if (owner != null) {
                            ownerLogin = owner.getLogin();
                            ownerType  = owner.getType();
                        }
                    } catch (Exception ignored) {}

                    String licenseKey = "";
                    try {
                        GHLicense lic = repo.getLicense();
                        if (lic != null && lic.getKey() != null) licenseKey = lic.getKey();
                    } catch (Exception ignored) {}

                    String topics = "";
                    try {
                        List<String> t = repo.listTopics();
                        topics = String.join(";", t);
                    } catch (Exception ignored) {}

                    String primaryLang = repo.getLanguage() == null ? "" : repo.getLanguage();
                    int subscribers    = safeInt(() -> repo.getSubscribersCount());
                    int openIssues     = safeInt(() -> repo.getOpenIssueCount());
                    int sizeKb         = repo.getSize(); // in KB
                    String homepage    = repo.getHomepage() == null ? "" : repo.getHomepage();
                    boolean hasWiki    = safeBool(() -> repo.hasWiki());
                    boolean hasPages   = safeBool(() -> repo.hasPages());

                    int releasesCount = fastReleasesCount(token, repo);
                    String lastReleaseDate = fetchLatestReleaseDate(token, repo);

                    String projectType = classifyProjectType(desc, readmeLower, repo);

                    printer.printRecord(
                            full,
                            repo.getHtmlUrl().toString(),
                            ownerLogin,
                            ownerType,
                            repo.getDefaultBranch(),

                            repo.getStargazersCount(),
                            repo.getForksCount(),
                            subscribers,
                            openIssues,
                            commitsTotal,
                            prTotal,
                            issueTotal,
                            contribs,

                            toIso(repo.getCreatedAt()),
                            toIso(repo.getPushedAt()),
                            toIso(repo.getUpdatedAt()),
                            lastReleaseDate,

                            primaryLang,
                            licenseKey,
                            topics,
                            sizeKb,
                            homepage,
                            hasWiki ? "Yes" : "No",
                            hasPages ? "Yes" : "No",
                            hasBuild ? "Yes" : "No",
                            hasCi ? "Yes" : "No",

                            "Yes",
                            signal,
                            projectType
                    );
                    printer.flush();

                    added++;
                    System.out.printf("  ✓ ADDED (%d/%d): %s [signal=%s, type=%s] (stars=%d, commits=%d, PRs=%d, issues=%d, contribs=%d, releases=%d)%n",
                            added, TARGET_COUNT, full, signal, projectType, repo.getStargazersCount(),
                            commitsTotal, prTotal, issueTotal, contribs, releasesCount);
                }
            }

            System.out.println("\nDone. Passing repositories: " + added);
            System.out.println("CSV written to: " + CSV_NAME);
        }
    }

    private static String verifyConcurrencyInRepo(GitHub github, String fullName) throws IOException {
        String[] checks = new String[] {
                "repo:" + fullName + " language:Java in:file extension:java \"parallelStream(\"",
                "repo:" + fullName + " language:Java in:file extension:java \"CompletableFuture\"",
                "repo:" + fullName + " language:Java in:file extension:java \"ExecutorService\"",
                "repo:" + fullName + " language:Java in:file extension:java \"ForkJoinPool\"",
                "repo:" + fullName + " language:Java in:file extension:java \"import java.util.concurrent\"",
                "repo:" + fullName + " language:Java in:file extension:java \"import java.util.stream\""
        };
        for (String q : checks) {
            PagedSearchIterable<GHContent> res = github.searchContent().q(q).list().withPageSize(1);
            for (GHContent ignored : res) {
                if (q.contains("parallelStream("))          return "parallelStream(";
                if (q.contains("CompletableFuture"))         return "CompletableFuture";
                if (q.contains("ExecutorService"))           return "ExecutorService";
                if (q.contains("ForkJoinPool"))              return "ForkJoinPool";
                if (q.contains("import java.util.concurrent")) return "import java.util.concurrent";
                if (q.contains("import java.util.stream"))   return "import java.util.stream";
                return "concurrency-match";
            }
        }
        return "";
    }

    private static boolean looksAcademic(String descriptionLower, String readmeLower) {
        String text = (descriptionLower + " " + readmeLower).trim();
        if (text.isEmpty()) return false;
        String[] bad = {
                "assignment", "homework", "course", "university", "semester",
                "lab ", " lab-", " lab_", "student", "tutorial", "lecturer",
                "professor", "school", "curriculum", "practical", "tp ", "tp-",
                "exercise", "tp_", "classwork"
        };
        return containsAny(text, bad);
    }

    private static String classifyProjectType(String descriptionLower, String readmeLower, GHRepository repo) throws IOException {
        String text = (descriptionLower + " " + readmeLower).trim();

        String[] academic = {
                "assignment", "homework", "course", "university", "semester", "lab ",
                " lab-", " lab_", "student", "tutorial", "lecturer", "professor", "school", "curriculum"
        };
        if (containsAny(text, academic)) return "Academic";

        String[] research = {
                "research", "paper", "arxiv", "dataset", "benchmark", "experiment",
                "thesis", "supplementary", "icse", "fse", "ase", "msr", "issta"
        };
        if (containsAny(text, research)) return "Research";

        boolean orgOwner = false;
        try {
            GHUser owner = repo.getOwner();
            orgOwner = owner != null && "Organization".equalsIgnoreCase(owner.getType());
        } catch (Exception ignored) {}

        String[] industrial = {
                "build status", "github actions", "travis-ci", "circleci", "azure pipelines",
                "maven central", "sonatype", "docker", "docker pull", "kubernetes", "helm",
                "production", "enterprise", "high performance", "scalable", "observability"
        };
        boolean industrialSignals = containsAny(text, industrial);
        boolean highStars = repo.getStargazersCount() >= INDUSTRIAL_STARS_THRESHOLD;

        if (orgOwner || industrialSignals || highStars) return "Industrial OSS";
        return "Open-source (general)";
    }

    // ---------- Cheap signals: build system and CI presence ----------
    private static boolean hasBuildSystem(GHRepository repo) {
        String[] files = { "pom.xml", "build.gradle", "build.gradle.kts", "settings.gradle", "gradlew" };
        for (String f : files) {
            if (exists(repo, f)) return true;
        }
        if (exists(repo, "parent/pom.xml")) return true;
        return false;
    }

    private static boolean hasCiConfig(GHRepository repo) {
        if (exists(repo, ".github/workflows")) return true;
        String[] files = { ".travis.yml", ".circleci/config.yml", "Jenkinsfile", "azure-pipelines.yml" };
        for (String f : files) if (exists(repo, f)) return true;
        return false;
    }

    private static boolean exists(GHRepository repo, String path) {
        try {
            GHContent c = repo.getFileContent(path);
            return c != null;
        } catch (IOException e) {
            if (path.endsWith("/")) return false;
            try {
                repo.getDirectoryContent(path);
                return true;
            } catch (IOException ignored) {
                return false;
            }
        }
    }

    // ---------- Fast counters (no heavy pagination) ----------
    private static int fastSearchIssuesTotal(GitHub github, String query) throws IOException {
        return github.searchIssues().q(query).list().getTotalCount();
    }

    private static int fastCommitCountAll(String token, GHRepository repo) {
        String branch = repo.getDefaultBranch();
        if (branch == null || branch.isEmpty()) branch = "main"; // fallback

        String apiUrl = String.format(
                "https://api.github.com/repos/%s/commits?per_page=1&sha=%s",
                repo.getFullName(),
                urlEncode(branch)
        );
        return lastPageCount(token, apiUrl);
    }

    private static int fastContributorsCount(String token, GHRepository repo) {
        String apiUrl = String.format(
                "https://api.github.com/repos/%s/contributors?per_page=1&anon=true",
                repo.getFullName()
        );
        return lastPageCount(token, apiUrl);
    }

    private static int fastReleasesCount(String token, GHRepository repo) {
        String apiUrl = String.format(
                "https://api.github.com/repos/%s/releases?per_page=1",
                repo.getFullName()
        );
        return lastPageCount(token, apiUrl);
    }

    private static String fetchLatestReleaseDate(String token, GHRepository repo) {
        try {
            String apiUrl = String.format(
                    "https://api.github.com/repos/%s/releases/latest",
                    repo.getFullName()
            );
            HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "token " + token);
            conn.setRequestProperty("Accept", "application/vnd.github+json");
            conn.setRequestProperty("User-Agent", "GitHubConcurrencyMiner");
            int code = conn.getResponseCode();
            if (code == 200) {
                String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                int i = body.indexOf("\"published_at\"");
                if (i >= 0) {
                    int colon = body.indexOf(':', i);
                    int quote1 = body.indexOf('"', colon + 1);
                    int quote2 = body.indexOf('"', quote1 + 1);
                    if (colon > 0 && quote1 > 0 && quote2 > quote1) {
                        return body.substring(quote1 + 1, quote2);
                    }
                }
            }
            return "";
        } catch (Exception e) {
            return "";
        }
    }

    private static int lastPageCount(String token, String apiUrl) {
        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "token " + token);
            conn.setRequestProperty("Accept", "application/vnd.github+json");
            conn.setRequestProperty("User-Agent", "GitHubConcurrencyMiner");
            int code = conn.getResponseCode();
            if (code == 200) {
                String link = conn.getHeaderField("Link");
                if (link == null || link.isEmpty()) {
                    try (InputStream in = conn.getInputStream()) {
                        byte[] buf = in.readNBytes(2);
                        return buf.length > 0 ? 1 : 0;
                    }
                }
                return extractLastPageNumber(link);
            } else if (code == 409) {
                return 0;
            } else {
                System.out.println("  • HTTP " + code + " for " + apiUrl);
                return 0;
            }
        } catch (Exception e) {
            System.out.println("  • Count error for " + apiUrl + ": " + e.getMessage());
            return 0;
        }
    }

    private static int extractLastPageNumber(String linkHeader) {
        String[] parts = linkHeader.split(",\\s*");
        for (String p : parts) {
            if (p.contains("rel=\"last\"")) {
                int i1 = p.indexOf('<'), i2 = p.indexOf('>');
                if (i1 >= 0 && i2 > i1) {
                    String url = p.substring(i1 + 1, i2);
                    int pos = url.lastIndexOf("page=");
                    if (pos >= 0) {
                        String num = url.substring(pos + 5);
                        int amp = num.indexOf('&');
                        if (amp >= 0) num = num.substring(0, amp);
                        try { return Integer.parseInt(num); } catch (NumberFormatException ignored) {}
                    }
                }
            }
        }
        for (String p : parts) {
            if (p.contains("rel=\"next\"") || p.contains("rel=\"prev\"")) return 2;
        }
        return 0;
    }

    private static String fetchReadmeLower(GHRepository repo) {
        try {
            GHContent readme = repo.getReadme();
            if (readme == null) return "";
            try (InputStream in = readme.read()) {
                byte[] bytes = in.readAllBytes();
                return new String(bytes, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            }
        } catch (Exception e) {
            return "";
        }
    }

    private static boolean containsAny(String haystack, String[] needles) {
        if (haystack == null || haystack.isEmpty()) return false;
        for (String n : needles) if (haystack.contains(n)) return true;
        return false;
    }

    private static String safeLower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    private static String toIso(Date d) {
        if (d == null) return "";
        return DateTimeFormatter.ISO_INSTANT.format(d.toInstant());
    }

    private static String urlEncode(String s) {
        try { return java.net.URLEncoder.encode(s, "UTF-8"); }
        catch (UnsupportedEncodingException e) { return s; }
    }

    private static int safeInt(IntSupplierThrowing f) {
        try { return f.getAsInt(); } catch (Exception e) { return 0; }
    }
    private static boolean safeBool(BoolSupplierThrowing f) {
        try { return f.getAsBool(); } catch (Exception e) { return false; }
    }
    @FunctionalInterface private interface IntSupplierThrowing { int getAsInt() throws Exception; }
    @FunctionalInterface private interface BoolSupplierThrowing { boolean getAsBool() throws Exception; }

    private static String shortQuery(String q) {
        return q.length() <= 60 ? q : q.substring(0, 57) + "...";
    }

    // ---------- Rate limit helper ----------
    private static class RateLimitChecker {
        private final GitHub gh;
        RateLimitChecker(GitHub gh) { this.gh = gh; }
        void ensure() throws IOException {
            GHRateLimit r = gh.getRateLimit();
            if (r.getRemaining() < 10) {
                long waitMs = Math.max(0, r.getResetDate().getTime() - System.currentTimeMillis() + 1500);
                System.out.printf("  • Rate limit low. Sleeping %d ms...%n", waitMs);
                try { Thread.sleep(waitMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }
}
