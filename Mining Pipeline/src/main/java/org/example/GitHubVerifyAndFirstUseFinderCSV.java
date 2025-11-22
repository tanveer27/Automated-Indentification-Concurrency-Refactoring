package org.example;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.kohsuke.github.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.*;
import java.util.regex.Pattern;

/**
 * GitHub Verify + First-Use Finder per Code Block (CSV output)
 *
 * Reads a CSV of GitHub repositories, searches for concurrency/parallelism usage in Java files,
 * verifies actual code usage, and finds the first introduction commit per code block.
 *
 * Outputs results to a CSV file with details per code block found.
 * Dataset A- part 2
 */
public class GitHubVerifyAndFirstUseFinderCSV {

    // ---------- Files ----------
    private static final String INPUT_CSV  = "github_concurrency_projects.csv"; // your CSV in project root
    private static final String OUTPUT_CSV = "verify_first_use.csv";

    // ---------- Comprehensive signals (non-import lines only) ----------
    private static final String[] DEFINITIVE_LITERALS = new String[] {
            // Streams & Parallel Streams
            "parallelStream(", ".parallel()", "BaseStream.parallel(",
            "Stream.of(", "Arrays.stream(", "StreamSupport.stream(", "Spliterator", "Spliterators",
            // IntStream / LongStream / DoubleStream + ranges
            "IntStream.", "LongStream.", "DoubleStream.",
            "IntStream.range(", "IntStream.rangeClosed(",
            "LongStream.range(", "LongStream.rangeClosed(",
            // Parallel-friendly collectors
            "Collectors.toConcurrentMap(", "Collectors.groupingByConcurrent(",
            // Executors & Pools
            "Executor ", "ExecutorService", "ScheduledExecutorService",
            "Executors.newFixedThreadPool(", "Executors.newCachedThreadPool(", "Executors.newSingleThreadExecutor(",
            "Executors.newSingleThreadScheduledExecutor(", "Executors.newScheduledThreadPool(",
            "Executors.newWorkStealingPool(", "Executors.newThreadPerTaskExecutor(", "Executors.newVirtualThreadPerTaskExecutor(",
            "ThreadPoolExecutor", "ForkJoinPool", "ForkJoinTask", "RecursiveAction", "RecursiveTask",
            // Futures & CompletableFuture / CompletionStage
            "Future<", "Future ", "CompletableFuture", "CompletionStage",
            "CompletableFuture.supplyAsync(", "CompletableFuture.runAsync(",
            "thenApplyAsync(", "thenAcceptAsync(", "thenRunAsync(",
            "thenComposeAsync(", "handleAsync(", "whenCompleteAsync(",
            "CompletableFuture.allOf(", "CompletableFuture.anyOf(",
            // Threads & Runnables
            "new Thread(", "Runnable", "Callable", "ThreadFactory", "ThreadLocal",
            // Locks & Conditions
            "ReentrantLock", "ReentrantReadWriteLock", "StampedLock", "Condition ",
            // Synchronizers
            "CountDownLatch", "CyclicBarrier", "Phaser", "Semaphore", "Exchanger",
            // Atomics & Adders
            "AtomicBoolean", "AtomicInteger", "AtomicLong", "AtomicReference",
            "AtomicIntegerArray", "AtomicLongArray", "AtomicReferenceArray",
            "LongAdder", "DoubleAdder",
            // Concurrent Collections
            "ConcurrentHashMap", "ConcurrentSkipListMap", "ConcurrentSkipListSet",
            "ConcurrentLinkedQueue", "ConcurrentLinkedDeque",
            "LinkedBlockingQueue", "ArrayBlockingQueue", "PriorityBlockingQueue", "SynchronousQueue",
            "LinkedTransferQueue", "DelayQueue", "TransferQueue",
            // Reactive (Flow)
            "Flow.Publisher", "Flow.Subscriber", "Flow.Processor", "Flow.Subscription", "SubmissionPublisher",
            // Language primitives (optional; keep if you want to catch low-level usage)
            "synchronized (", " synchronized ", " volatile "
    };

    private static final Pattern[] DEFINITIVE_REGEX = new Pattern[] {
            // parallel toggles on streams
            Pattern.compile("\\.parallel\\s*\\(\\)"),
            Pattern.compile("\\bparallelStream\\s*\\("),
            Pattern.compile("\\bBaseStream\\s*\\.\\s*parallel\\s*\\("),
            // Stream creation
            Pattern.compile("\\bStreamSupport\\s*\\.\\s*stream\\s*\\("),
            Pattern.compile("\\bArrays\\s*\\.\\s*stream\\s*\\("),
            Pattern.compile("\\bStream\\s*\\.\\s*of\\s*\\("),
            // Int/Long/Double streams + ranges
            Pattern.compile("\\bIntStream\\s*\\.\\s*range(Closed)?\\s*\\("),
            Pattern.compile("\\bLongStream\\s*\\.\\s*range(Closed)?\\s*\\("),
            Pattern.compile("\\b(DoubleStream|IntStream|LongStream)\\s*\\."),
            // Collectors concurrent
            Pattern.compile("\\bCollectors\\s*\\.\\s*toConcurrentMap\\s*\\("),
            Pattern.compile("\\bCollectors\\s*\\.\\s*groupingByConcurrent\\s*\\("),
            // Executors & pools
            Pattern.compile("\\bExecutors\\s*\\.\\s*new(FixedThreadPool|CachedThreadPool|SingleThreadExecutor|SingleThreadScheduledExecutor|ScheduledThreadPool|WorkStealingPool|ThreadPerTaskExecutor|VirtualThreadPerTaskExecutor)\\s*\\("),
            Pattern.compile("\\b(ThreadPoolExecutor|ForkJoinPool|ForkJoinTask|Recursive(Action|Task))\\b"),
            Pattern.compile("\\b(ScheduledExecutorService|ExecutorService|Executor)\\b"),
            // Future / CF / Stage
            Pattern.compile("\\bCompletableFuture\\s*\\.\\s*(supplyAsync|runAsync|allOf|anyOf)\\s*\\("),
            Pattern.compile("\\b(thenApplyAsync|thenAcceptAsync|thenRunAsync|thenComposeAsync|handleAsync|whenCompleteAsync)\\s*\\("),
            Pattern.compile("\\b(CompletionStage|Future)\\b"),
            // Threads
            Pattern.compile("\\bnew\\s+Thread\\s*\\("),
            Pattern.compile("\\b(Runnable|Callable|ThreadFactory|ThreadLocal)\\b"),
            // Locks & conditions
            Pattern.compile("\\b(ReentrantLock|ReentrantReadWriteLock|StampedLock|Condition)\\b"),
            // Synchronizers
            Pattern.compile("\\b(CountDownLatch|CyclicBarrier|Phaser|Semaphore|Exchanger)\\b"),
            // Atomics & Adders
            Pattern.compile("\\bAtomic(Boolean|Integer|Long|Reference)(Array)?\\b"),
            Pattern.compile("\\b(LongAdder|DoubleAdder)\\b"),
            // Concurrent collections
            Pattern.compile("\\bConcurrent(HashMap|SkipListMap|SkipListSet|LinkedQueue|LinkedDeque)\\b"),
            Pattern.compile("\\b(LinkedBlockingQueue|ArrayBlockingQueue|PriorityBlockingQueue|SynchronousQueue|LinkedTransferQueue|DelayQueue|TransferQueue)\\b"),
            // Reactive (Flow)
            Pattern.compile("\\bFlow\\s*\\.\\s*(Publisher|Subscriber|Processor|Subscription)\\b"),
            Pattern.compile("\\bSubmissionPublisher\\b"),
            // Language primitives
            Pattern.compile("\\bsynchronized\\b"),
            Pattern.compile("\\bvolatile\\b")
    };

    // If only imports match in search, we’ll still fetch and verify code usage
    private static final String[] CONC_IMPORT_HINTS = new String[]{
            "import java.util.concurrent", "import java.util.stream"
    };

    // Runtime bounds
    private static final int MAX_FILES_PER_REPO   = 12;
    private static final int MAX_COMMITS_PER_FILE = 400;
    private static final int PAGE_SIZE            = 50;
    private static final int SNIPPET_CONTEXT      = 2;
    private static final int MAX_SNIPPET_CHARS    = 600;

    // ---------- Models ----------
    private static class RepoEntry {
        final String fullName, htmlUrl;
        RepoEntry(String n, String u) { fullName = n; htmlUrl = u; }
    }
    private static class CodeBlock {
        final int lineIndex;
        final String lineText;
        final String normSignature;
        final String signal;
        CodeBlock(int lineIndex, String lineText, String normSignature, String signal) {
            this.lineIndex=lineIndex; this.lineText=lineText; this.normSignature=normSignature; this.signal=signal;
        }
    }
    private static class BlockIntro {
        final String sha, whenUtc, codeBlock, signal;
        final int line; // 1-based
        BlockIntro(String sha, String whenUtc, String codeBlock, String signal, int line) {
            this.sha=sha; this.whenUtc=whenUtc; this.codeBlock=codeBlock; this.signal=signal; this.line=line;
        }
    }
    private static class FirstIntro {
        final List<BlockIntro> blockIntros = new ArrayList<>();
    }

    // ---------- Entry point ----------
    public static void main(String[] args) throws Exception {
        String token = System.getenv("GITHUB_TOKEN");
        if (token == null || token.isEmpty()) {
            System.err.println("ERROR: Set GITHUB_TOKEN first.");
            return;
        }
        GitHub gh = new GitHubBuilder().withOAuthToken(token).build();
        RateLimiter rate = new RateLimiter(gh);

        File input = new File(System.getProperty("user.dir"), INPUT_CSV);
        if (!input.exists()) {
            System.err.println("Input CSV not found at: " + input.getAbsolutePath());
            return;
        }
        System.out.println("[Verify+FirstUse/Per-Block] Input: " + input.getAbsolutePath());

        List<RepoEntry> repos = readReposFlexible(input);

        try (OutputStream fos = new FileOutputStream(OUTPUT_CSV);
             Writer out = new OutputStreamWriter(new BufferedOutputStream(fos), StandardCharsets.UTF_8);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT.withHeader(
                     "Repository","Repo URL",
                     "Commit SHA","Commit Date (UTC)","Commit URL",
                     "File Path","Line","File URL at Commit",
                     "Signal","Category",
                     "Code Snippet","Short Description"
             ))) {
            out.write('\uFEFF'); // Excel BOM

            for (RepoEntry re : repos) {
                System.out.println("\n=== Repo: " + re.fullName + " ===");
                GHRepository repo;
                try {
                    rate.ensure();
                    repo = gh.getRepository(re.fullName);
                } catch (Exception e) {
                    System.out.println("  • Skip (cannot access): " + e.getMessage());
                    continue;
                }
                if (repo == null) continue;

                // Seed candidate files using search
                List<String> candidatePaths = seedCandidateFiles(gh, repo, rate, MAX_FILES_PER_REPO);

                if (candidatePaths.isEmpty()) {
                    printer.printRecord(re.fullName, re.htmlUrl, "", "", "",
                            "", "", "", "", "", "", "No definitive usage found (only imports or none)");
                    printer.flush();
                    continue;
                }

                String branch = safeDefaultBranch(repo);

                for (String path : candidatePaths) {
                    String headContent = fetchRaw(token, re.fullName, branch, path);
                    if (headContent == null) continue;

                    List<CodeBlock> blocks = collectAllBlocks(headContent);
                    if (blocks.isEmpty()) continue;

                    FirstIntro first;
                    try {
                        first = findFirstIntroductionOfBlock(token, repo, path, blocks, rate);
                    } catch (Exception ex) {
                        System.out.println("    • History scan error: " + ex.getMessage());
                        continue;
                    }

                    for (BlockIntro bi : first.blockIntros) {
                        String fileUrlAtCommit = blobUrl(re.fullName, bi.sha, path, bi.line);
                        String commitUrl = commitUrl(re.fullName, bi.sha);
                        String category = categorizeSignal(bi.signal);
                        String shortDesc = shortDescription(bi.signal, category);

                        printer.printRecord(
                                re.fullName, re.htmlUrl,
                                bi.sha, bi.whenUtc, commitUrl,
                                path, bi.line, fileUrlAtCommit,
                                bi.signal, category,
                                truncate(bi.codeBlock, MAX_SNIPPET_CHARS),
                                shortDesc
                        );
                    }
                    printer.flush();
                }
            }

            System.out.println("\n✅ CSV written: " + OUTPUT_CSV);
        }
    }

    /* ---------- Flexible CSV reading ----------
    */
    private static List<RepoEntry> readReposFlexible(File file) throws IOException {
        try (Reader r = new FileReader(file, StandardCharsets.UTF_8);
             CSVParser p = new CSVParser(r, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            Map<String,Integer> header = p.getHeaderMap();
            String repoCol = firstHeader(header.keySet(),
                    "Repository","Repo","FullName","Full Name","owner/name","Full_Name","Name");
            String urlCol = firstHeader(header.keySet(),
                    "URL","Repo URL","HtmlUrl","HTML URL","Link");

            if (repoCol == null) {
                throw new IOException("Could not find a repo name column. Present headers: " + header.keySet());
            }

            List<RepoEntry> out = new ArrayList<>();
            for (CSVRecord rec : p) {
                String repo = safe(rec.get(repoCol));
                if (repo.isEmpty()) continue;

                String url = "";
                if (urlCol != null) url = safe(rec.get(urlCol));
                if (url.isEmpty()) url = "https://github.com/" + repo;

                out.add(new RepoEntry(repo, url));
            }
            return out;
        }
    }

    private static String firstHeader(Set<String> headers, String... cs) {
        for (String c : cs) for (String h : headers) if (h.equalsIgnoreCase(c)) return h;
        return null;
    }
    private static String safe(String s) { return s == null ? "" : s.trim(); }

    // ---------- Seed candidate files via search ----------
    private static List<String> seedCandidateFiles(GitHub gh, GHRepository repo, RateLimiter rate, int cap) {
        Set<String> paths = new LinkedHashSet<>();
        for (String lit : DEFINITIVE_LITERALS) {
            if (paths.size() >= cap) break;
            String q = "repo:" + repo.getFullName() + " language:Java in:file extension:java \"" + lit.replace("\"","\\\"") + "\"";
            try {
                rate.ensure();
                PagedSearchIterable<GHContent> res = gh.searchContent().q(q).list().withPageSize(PAGE_SIZE);
                for (GHContent c : res) {
                    if (paths.size() >= cap) break;
                    String path = c.getPath();
                    if (path != null && path.endsWith(".java")) paths.add(path);
                }
            } catch (Exception ignored) {}
        }
        if (paths.isEmpty()) {
            for (String hint : CONC_IMPORT_HINTS) {
                if (paths.size() >= cap) break;
                String q = "repo:" + repo.getFullName() + " language:Java in:file extension:java \"" + hint + "\"";
                try {
                    rate.ensure();
                    PagedSearchIterable<GHContent> res = gh.searchContent().q(q).list().withPageSize(PAGE_SIZE);
                    for (GHContent c : res) {
                        if (paths.size() >= cap) break;
                        String path = c.getPath();
                        if (path != null && path.endsWith(".java")) paths.add(path);
                    }
                } catch (Exception ignored) {}
            }
        }
        return new ArrayList<>(paths);
    }

    // ---------- Collect ALL blocks at HEAD (token-normalized + logical lines) ----------
    private static class LogicalLine {
        final int startIdx;
        final int endIdx;
        final String text;
        LogicalLine(int s, int e, String t){ startIdx=s; endIdx=e; text=t; }
    }

    private static List<CodeBlock> collectAllBlocks(String content) {
        List<CodeBlock> out = new ArrayList<>();
        if (content == null) return out;

        String noComments = stripComments(content);
        String[] lines = noComments.split("\\R");

        int i = 0;
        while (i < lines.length) {
            String rawLine = lines[i];
            String trimmed = rawLine.trim();

            if (trimmed.isEmpty() || trimmed.startsWith("import ") || trimmed.startsWith("*")) {
                i++;
                continue;
            }

            LogicalLine ll = buildLogicalLine(lines, i);
            String logical = ll.text;
            String logicalTrim = logical.trim();
            if (logicalTrim.isEmpty()) { i = ll.endIdx + 1; continue; }

            String signal = null;
            String checkText = logicalTrim;
            for (String lit : DEFINITIVE_LITERALS) { if (checkText.contains(lit)) { signal = lit; break; } }
            if (signal == null) {
                for (Pattern p : DEFINITIVE_REGEX) { if (p.matcher(checkText).find()) { signal = p.pattern(); break; } }
            }

            if (signal != null) {
                String norm = signatureOf(checkText);
                out.add(new CodeBlock(ll.startIdx, rawLine, norm, signal));
            }

            i = ll.endIdx + 1;
        }
        return out;
    }

    private static LogicalLine buildLogicalLine(String[] lines, int start) {
        StringBuilder sb = new StringBuilder();
        int depthParen = 0, depthBrace = 0, depthBracket = 0;
        int end = start;

        for (int i = start; i < lines.length; i++) {
            String ln = lines[i];
            sb.append(ln).append('\n');

            for (int k = 0; k < ln.length(); k++) {
                char c = ln.charAt(k);
                if (c == '(') depthParen++;
                else if (c == ')') depthParen = Math.max(0, depthParen - 1);
                else if (c == '{') depthBrace++;
                else if (c == '}') depthBrace = Math.max(0, depthBrace - 1);
                else if (c == '[') depthBracket++;
                else if (c == ']') depthBracket = Math.max(0, depthBracket - 1);
            }

            String t = ln.trim();
            boolean likelyContinues = t.endsWith(".") || t.endsWith(",") || t.endsWith("->");
            if (depthParen == 0 && depthBrace == 0 && depthBracket == 0 && !likelyContinues) {
                end = i; break;
            }
            end = i;
        }
        return new LogicalLine(start, end, sb.toString());
    }

    /*
        ---------- Find first introduction of each code block ----------
     */
    private static FirstIntro findFirstIntroductionOfBlock(String token, GHRepository repo, String path,
                                                           List<CodeBlock> headBlocks, RateLimiter rate) throws IOException {
        Map<String, List<CodeBlock>> sigToBlocks = new LinkedHashMap<>();
        for (CodeBlock b : headBlocks) {
            sigToBlocks.computeIfAbsent(b.normSignature, k -> new ArrayList<>()).add(b);
        }

        List<GHCommit> commits = new ArrayList<>();
        try {
            rate.ensure();
            PagedIterable<GHCommit> iter = repo.queryCommits().path(path).list().withPageSize(PAGE_SIZE);
            for (GHCommit c : iter) {
                commits.add(c);
                if (commits.size() >= MAX_COMMITS_PER_FILE) break;
            }
        } catch (Exception e) {
            throw new RuntimeException("commit history error: " + e.getMessage(), e);
        }
        Collections.reverse(commits);

        Map<String, BlockIntro> firsts = new HashMap<>();
        Set<String> prevHas = new HashSet<>();

        for (int idx = 0; idx < commits.size(); idx++) {
            GHCommit commit = commits.get(idx);
            String sha = commit.getSHA1();

            String content = fetchRawAtCommit(token, repo.getFullName(), sha, path);
            if (content == null) continue;

            Map<String, Integer> sigLineMap = new HashMap<>();
            String noComments = stripComments(content);
            String[] lines = noComments.split("\\R");

            int j = 0;
            while (j < lines.length) {
                String raw = lines[j].trim();
                if (raw.isEmpty() || raw.startsWith("import ") || raw.startsWith("*")) { j++; continue; }

                LogicalLine ll = buildLogicalLine(lines, j);
                String logical = ll.text.trim();
                if (!logical.isEmpty()) {
                    String norm = signatureOf(logical);
                    if (sigToBlocks.containsKey(norm)) {
                        if (lineHasAnySignal(logical)) {
                            sigLineMap.put(norm, ll.startIdx + 1); // 1-based
                        }
                    }
                }
                j = ll.endIdx + 1;
            }
            Set<String> nowHas = sigLineMap.keySet();

            for (String sig : sigToBlocks.keySet()) {
                if (firsts.containsKey(sig)) continue;
                boolean hadBefore = prevHas.contains(sig);
                boolean hasNow = nowHas.contains(sig);
                if (!hadBefore && hasNow) {
                    GHCommit.ShortInfo info = commit.getCommitShortInfo();
                    Date when = null;
                    String msg = "";
                    try {
                        if (info != null) {
                            msg = info.getMessage() == null ? "" : info.getMessage();
                            if (info.getAuthor() != null) when = info.getAuthor().getDate();
                            else if (info.getCommitter() != null) when = info.getCommitter().getDate();
                        }
                    } catch (Exception ignored) {}

                    int lineOneBased = sigLineMap.getOrDefault(sig, 0);
                    String block = extractCodeBlock(content, Math.max(1, lineOneBased) - 1, SNIPPET_CONTEXT);

                    String signal = pickSignalForSignature(sigToBlocks.get(sig));
                    String whenStr = (when == null) ? "" : when.toInstant().atZone(ZoneOffset.UTC).toString();
                    firsts.put(sig, new BlockIntro(sha, whenStr, block, signal, lineOneBased));
                }
            }
            prevHas = nowHas;
            if (firsts.size() == sigToBlocks.size()) break;
        }

        FirstIntro result = new FirstIntro();
        for (Map.Entry<String, List<CodeBlock>> e : sigToBlocks.entrySet()) {
            String sig = e.getKey();
            BlockIntro bi = firsts.get(sig);
            if (bi == null) {
                for (CodeBlock b : e.getValue()) {
                    result.blockIntros.add(new BlockIntro(
                            "", "", extractHeadSingleLine(b), b.signal, b.lineIndex + 1
                    ));
                }
            } else {
                for (CodeBlock b : e.getValue()) {
                    result.blockIntros.add(new BlockIntro(
                            bi.sha, bi.whenUtc, bi.codeBlock, b.signal, bi.line
                    ));
                }
            }
        }
        return result;
    }

    private static boolean lineHasAnySignal(String t) {
        for (String lit : DEFINITIVE_LITERALS) if (t.contains(lit)) return true;
        for (Pattern p : DEFINITIVE_REGEX) if (p.matcher(t).find()) return true;
        return false;
    }

    private static String pickSignalForSignature(List<CodeBlock> blocks) {
        if (blocks == null || blocks.isEmpty()) return "Concurrency / Parallelism";
        List<String> order = Arrays.asList("parallelStream(", ".parallel()", "CompletableFuture", "Executor", "ForkJoin", "IntStream", "LongStream", "Stream.");
        for (String want : order) {
            for (CodeBlock b : blocks) if (b.signal.contains(want)) return b.signal;
        }
        return blocks.get(0).signal;
    }

    private static String extractCodeBlock(String content, int centerIndex, int context) {
        String[] lines = content.split("\\R");
        int start = Math.max(0, centerIndex - context);
        int end = Math.min(lines.length - 1, centerIndex + context);
        StringBuilder sb = new StringBuilder();
        for (int i = start; i <= end; i++) {
            String prefix = (i == centerIndex) ? ">> " : "   ";
            sb.append(prefix).append(i + 1).append(": ")
                    .append(lines[i].replace("\t","    "))
                    .append("\n");
        }
        String s = sb.toString().trim();
        return s.length() > MAX_SNIPPET_CHARS ? s.substring(0, MAX_SNIPPET_CHARS - 3) + "..." : s;
    }

    private static String extractHeadSingleLine(CodeBlock b) {
        return ">> " + (b.lineIndex + 1) + ": " + b.lineText;
    }


    private static String stripComments(String src) {
        if (src == null || src.isEmpty()) return "";
        String noBlock = src.replaceAll("(?s)/\\*.*?\\*/", "");
        StringBuilder sb = new StringBuilder(noBlock.length());
        String[] lines = noBlock.split("\\R");
        for (String line : lines) {
            int idx = line.indexOf("//");
            sb.append(idx >= 0 ? line.substring(0, idx) : line).append('\n');
        }
        return sb.toString();
    }

    private static List<String> tokenizeJava(String s) {
        List<String> toks = new ArrayList<>();
        if (s == null) return toks;
        var m = Pattern
                .compile("[A-Za-z_][A-Za-z0-9_]*|\\d+|[\\.\\(\\)\\[\\]\\{\\},:;<>!=+\\-*/&|?]")
                .matcher(s);
        while (m.find()) toks.add(m.group());
        return toks;
    }

    private static String signatureOf(String s) {
        List<String> toks = tokenizeJava(s);
        StringBuilder sb = new StringBuilder();
        for (String t : toks) {
            sb.append(t.matches("[A-Za-z_][A-Za-z0-9_]*") ? t.toLowerCase() : t);
        }
        return sb.toString();
    }

    private static String fetchRaw(String token, String fullName, String branch, String path) {
        try {
            String rawUrl = "https://raw.githubusercontent.com/" + fullName + "/" + branch + "/" + path;
            HttpURLConnection conn = (HttpURLConnection) new URL(rawUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "token " + token);
            conn.setRequestProperty("User-Agent", "VerifyFirstUsePerBlock");
            int code = conn.getResponseCode();
            if (code == 200) {
                return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static String fetchRawAtCommit(String token, String fullName, String sha, String path) {
        try {
            String rawUrl = "https://raw.githubusercontent.com/" + fullName + "/" + sha + "/" + path;
            HttpURLConnection conn = (HttpURLConnection) new URL(rawUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "token " + token);
            conn.setRequestProperty("User-Agent", "VerifyFirstUsePerBlock");
            int code = conn.getResponseCode();
            if (code == 200) {
                return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static String commitUrl(String fullName, String sha) {
        return "https://github.com/" + fullName + "/commit/" + sha;
    }
    private static String blobUrl(String fullName, String sha, String path, int lineOneBased) {
        String base = "https://github.com/" + fullName + "/blob/" + sha + "/" + path;
        return (sha == null || sha.isBlank()) ? base : (lineOneBased > 0 ? (base + "#L" + lineOneBased) : base);
    }

    private static String safeDefaultBranch(GHRepository repo) {
        try {
            String b = repo.getDefaultBranch();
            if (b == null || b.isEmpty()) {
                Map<String, GHBranch> branches = repo.getBranches();
                if (branches != null && branches.containsKey("master")) return "master";
                return "main";
            }
            return b;
        } catch (Exception e) { return "main"; }
    }


    private static String categorizeSignal(String sig) {
        if (sig == null) return "Unknown";
        String s = sig;
        if (s.contains("parallelStream(") || s.contains(".parallel()") || s.contains("BaseStream.parallel"))
            return "Parallel Streams";
        if (s.contains("IntStream") || s.contains("LongStream") || s.contains("DoubleStream"))
            return "Streams (Primitive/Ranges)";
        if (s.contains("Stream.") || s.contains("Arrays.stream") || s.contains("StreamSupport.stream"))
            return "Streams";
        if (s.contains("Collectors.toConcurrentMap") || s.contains("groupingByConcurrent"))
            return "Concurrent Collectors";
        if (s.contains("CompletableFuture") || s.contains("CompletionStage") ||
                (s.contains("then") && s.contains("Async")))
            return "CompletableFuture / Async";
        if (s.contains("Executor") || s.contains("Executors.") || s.contains("ThreadPoolExecutor"))
            return "Executors / Thread Pools";
        if (s.contains("ForkJoin")) return "Fork/Join";
        if (s.contains("new Thread(") || s.contains("Runnable") || s.contains("Callable"))
            return "Threads";
        if (s.contains("Reentrant") || s.contains("StampedLock") || s.contains("Condition"))
            return "Locks";
        if (s.contains("CountDownLatch") || s.contains("CyclicBarrier") || s.contains("Phaser") ||
                s.contains("Semaphore") || s.contains("Exchanger"))
            return "Synchronizers";
        if (s.contains("Atomic") || s.contains("LongAdder") || s.contains("DoubleAdder"))
            return "Atomics / Adders";
        if (s.contains("Concurrent")) return "Concurrent Collections";
        if (s.contains("Flow.") || s.contains("SubmissionPublisher"))
            return "Reactive (Flow)";
        if (s.contains("synchronized") || s.matches(".*\\bvolatile\\b.*"))
            return "Language Primitives";
        return "Concurrency / Parallelism";
    }

    private static String shortDescription(String sig, String category) {
        switch (category) {
            case "Parallel Streams": return "Introduces parallel processing on a stream.";
            case "Streams (Primitive/Ranges)": return "Uses primitive streams or range operations.";
            case "Streams": return "Uses Java streams for data processing.";
            case "Concurrent Collectors": return "Collects results with thread-safe collectors.";
            case "CompletableFuture / Async": return "Adds asynchronous tasks/composition.";
            case "Executors / Thread Pools": return "Uses an Executor/Thread pool for concurrency.";
            case "Fork/Join": return "Parallel decomposition via Fork/Join framework.";
            case "Threads": return "Spawns or manages explicit threads.";
            case "Locks": return "Uses explicit locking/read-write/stamped locks.";
            case "Synchronizers": return "Coordinates threads with latches/barriers/semaphores.";
            case "Atomics / Adders": return "Employs atomic variables or adders.";
            case "Concurrent Collections": return "Relies on thread-safe collections/queues.";
            case "Reactive (Flow)": return "Implements reactive streams (Flow).";
            case "Language Primitives": return "Uses synchronized/volatile for concurrency control.";
            default: return "Introduces a concurrency/parallelism construct.";
        }
    }

    private static String truncate(String s, int n) {
        if (s == null) return "";
        s = s.trim();
        return s.length() <= n ? s : s.substring(0, n - 3) + "...";
    }

    /*
        ---------- Rate limiter ----------
     */
    private static class RateLimiter {
        private final GitHub gh;
        RateLimiter(GitHub gh) { this.gh = gh; }
        void ensure() throws IOException {
            GHRateLimit r = gh.getRateLimit();
            if (r.getRemaining() < 10) {
                long waitMs = Math.max(0, r.getResetDate().getTime() - System.currentTimeMillis() + 1500);
                System.out.printf("  • Rate limit low. Sleeping %,d ms…%n", waitMs);
                try { Thread.sleep(waitMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }
}
