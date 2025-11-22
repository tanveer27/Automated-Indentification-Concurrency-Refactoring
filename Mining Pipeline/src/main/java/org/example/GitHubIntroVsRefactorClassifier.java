package org.example;

import org.apache.commons.csv.*;
import org.kohsuke.github.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Classifies commits that introduced concurrency/parallelism constructs
 * as either "first-introduction" (new code) or "refactor" (from sequential).
 *
 * Input:  CSV with commit/file/signal info (from previous step)
 * Output: CSV with classification and code snippets
 * Task A: For each commit, fetch file at commit and parent, locate method, check for signals
 * Task B: Classify as "first-introduction" or "refactor"
 * Task C: Extract code snippets for context
 * Task D: Write output CSV
 *
 * Dataset A - part 3
 */

public class GitHubIntroVsRefactorClassifier {

    // ===== Input/Output =====
    private static final String INPUT_CSV  = "verify_first_use.csv";          // produced by your previous step
    private static final String OUTPUT_CSV = "verify_first_use_classified.csv";

    // ===== Signals =====
    private static final String[] DEFINITIVE_LITERALS = new String[] {
            "parallelStream(", ".parallel()", "BaseStream.parallel(",
            "Stream.of(", "Arrays.stream(", "StreamSupport.stream(", "Spliterator", "Spliterators",
            "IntStream.", "LongStream.", "DoubleStream.",
            "IntStream.range(", "IntStream.rangeClosed(", "LongStream.range(", "LongStream.rangeClosed(",
            "Collectors.toConcurrentMap(", "Collectors.groupingByConcurrent(",
            "Executor ", "ExecutorService", "ScheduledExecutorService",
            "Executors.newFixedThreadPool(", "Executors.newCachedThreadPool(", "Executors.newSingleThreadExecutor(",
            "Executors.newSingleThreadScheduledExecutor(", "Executors.newScheduledThreadPool(",
            "Executors.newWorkStealingPool(", "Executors.newThreadPerTaskExecutor(", "Executors.newVirtualThreadPerTaskExecutor(",
            "ThreadPoolExecutor", "ForkJoinPool", "ForkJoinTask", "RecursiveAction", "RecursiveTask",
            "Future<", "Future ", "CompletableFuture", "CompletionStage",
            "CompletableFuture.supplyAsync(", "CompletableFuture.runAsync(",
            "thenApplyAsync(", "thenAcceptAsync(", "thenRunAsync(",
            "thenComposeAsync(", "handleAsync(", "whenCompleteAsync(",
            "CompletableFuture.allOf(", "CompletableFuture.anyOf(",
            "new Thread(", "Runnable", "Callable", "ThreadFactory", "ThreadLocal",
            "ReentrantLock", "ReentrantReadWriteLock", "StampedLock", "Condition ",
            "CountDownLatch", "CyclicBarrier", "Phaser", "Semaphore", "Exchanger",
            "AtomicBoolean", "AtomicInteger", "AtomicLong", "AtomicReference",
            "AtomicIntegerArray", "AtomicLongArray", "AtomicReferenceArray",
            "LongAdder", "DoubleAdder",
            "ConcurrentHashMap", "ConcurrentSkipListMap", "ConcurrentSkipListSet",
            "ConcurrentLinkedQueue", "ConcurrentLinkedDeque",
            "LinkedBlockingQueue", "ArrayBlockingQueue", "PriorityBlockingQueue", "SynchronousQueue",
            "LinkedTransferQueue", "DelayQueue", "TransferQueue",
            "Flow.Publisher", "Flow.Subscriber", "Flow.Processor", "Flow.Subscription", "SubmissionPublisher",
            "synchronized (", " synchronized ", " volatile "
    };

    private static final Pattern[] DEFINITIVE_REGEX = new Pattern[] {
            Pattern.compile("\\.parallel\\s*\\(\\)"),
            Pattern.compile("\\bparallelStream\\s*\\("),
            Pattern.compile("\\bBaseStream\\s*\\.\\s*parallel\\s*\\("),
            Pattern.compile("\\bStreamSupport\\s*\\.\\s*stream\\s*\\("),
            Pattern.compile("\\bArrays\\s*\\.\\s*stream\\s*\\("),
            Pattern.compile("\\bStream\\s*\\.\\s*of\\s*\\("),
            Pattern.compile("\\bIntStream\\s*\\.\\s*range(Closed)?\\s*\\("),
            Pattern.compile("\\bLongStream\\s*\\.\\s*range(Closed)?\\s*\\("),
            Pattern.compile("\\b(DoubleStream|IntStream|LongStream)\\s*\\."),
            Pattern.compile("\\bCollectors\\s*\\.\\s*toConcurrentMap\\s*\\("),
            Pattern.compile("\\bCollectors\\s*\\.\\s*groupingByConcurrent\\s*\\("),
            Pattern.compile("\\bExecutors\\s*\\.\\s*new(FixedThreadPool|CachedThreadPool|SingleThreadExecutor|SingleThreadScheduledExecutor|ScheduledThreadPool|WorkStealingPool|ThreadPerTaskExecutor|VirtualThreadPerTaskExecutor)\\s*\\("),
            Pattern.compile("\\b(ThreadPoolExecutor|ForkJoinPool|ForkJoinTask|Recursive(Action|Task))\\b"),
            Pattern.compile("\\b(ScheduledExecutorService|ExecutorService|Executor)\\b"),
            Pattern.compile("\\bCompletableFuture\\s*\\.\\s*(supplyAsync|runAsync|allOf|anyOf)\\s*\\("),
            Pattern.compile("\\b(thenApplyAsync|thenAcceptAsync|thenRunAsync|thenComposeAsync|handleAsync|whenCompleteAsync)\\s*\\("),
            Pattern.compile("\\b(CompletionStage|Future)\\b"),
            Pattern.compile("\\bnew\\s+Thread\\s*\\("),
            Pattern.compile("\\b(Runnable|Callable|ThreadFactory|ThreadLocal)\\b"),
            Pattern.compile("\\b(ReentrantLock|ReentrantReadWriteLock|StampedLock|Condition)\\b"),
            Pattern.compile("\\b(CountDownLatch|CyclicBarrier|Phaser|Semaphore|Exchanger)\\b"),
            Pattern.compile("\\bAtomic(Boolean|Integer|Long|Reference)(Array)?\\b"),
            Pattern.compile("\\b(LongAdder|DoubleAdder)\\b"),
            Pattern.compile("\\bConcurrent(HashMap|SkipListMap|SkipListSet|LinkedQueue|LinkedDeque)\\b"),
            Pattern.compile("\\b(LinkedBlockingQueue|ArrayBlockingQueue|PriorityBlockingQueue|SynchronousQueue|LinkedTransferQueue|DelayQueue|TransferQueue)\\b"),
            Pattern.compile("\\bFlow\\s*\\.\\s*(Publisher|Subscriber|Processor|Subscription)\\b"),
            Pattern.compile("\\bSubmissionPublisher\\b"),
            Pattern.compile("\\bsynchronized\\b"),
            Pattern.compile("\\bvolatile\\b")
    };

    private static final Pattern SEQ_LOOP_HINT = Pattern.compile("\\b(for|while)\\s*\\(");
    private static final Pattern FOREACH_NO_PAR_HINT = Pattern.compile("\\.forEach\\s*\\("); // not necessarily stream

    // Method signature regex
    private static final Pattern METHOD_SIG =
            Pattern.compile("^\\s*(?:public|protected|private)?\\s*(?:static\\s+)?[\\w\\<\\>\\[\\]]+\\s+(\\w+)\\s*\\(([^)]*)\\)\\s*(?:throws\\s+[^{]+)?\\{\\s*$");

    // Limits
    private static final int CONTEXT = 3;
    private static final int MAX_SNIPPET = 900;

    public static void main(String[] args) throws Exception {
        String token = System.getenv("GITHUB_TOKEN");
        if (token == null || token.isEmpty()) {
            System.err.println("ERROR: set GITHUB_TOKEN first.");
            return;
        }

        GitHub gh = new GitHubBuilder().withOAuthToken(token).build();
        RateLimiter rate = new RateLimiter(gh);

        File input = new File(System.getProperty("user.dir"), INPUT_CSV);
        if (!input.exists()) {
            System.err.println("Input not found: " + input.getAbsolutePath());
            return;
        }

        try (Reader r = new FileReader(input, StandardCharsets.UTF_8);
             CSVParser p = new CSVParser(r, CSVFormat.DEFAULT.withFirstRecordAsHeader());
             OutputStream fos = new FileOutputStream(OUTPUT_CSV);
             Writer w = new OutputStreamWriter(new BufferedOutputStream(fos), StandardCharsets.UTF_8);
             CSVPrinter out = new CSVPrinter(w, CSVFormat.DEFAULT.withHeader(
                     "Repository","Repo URL",
                     "Commit SHA","Commit URL","Commit Date (UTC)",
                     "Parent SHA","Parent URL",
                     "File Path","Method Signature","Classification",
                     "Signal","Category",
                     "Original Code","Refactored Code",
                     "Rationale"
             ))) {

            w.write('\uFEFF'); // Excel BOM

            for (CSVRecord rec : p) {
                String repoFull = safe(rec.get("Repository"));
                String repoUrl  = safeOr(repDeriveUrl(repoFull, safe(rec.get("Repo URL")), safe(rec.get("Commit URL"))));
                String sha      = safe(rec.get("Commit SHA"));
                String filePath = safe(rec.get("File Path"));
                String signal   = safe(rec.get("Signal"));
                if (repoFull.isEmpty() || sha.isEmpty() || filePath.isEmpty()) continue;

                GHRepository repo;
                try { rate.ensure(); repo = gh.getRepository(repoFull); }
                catch (Exception e) { continue; }
                GHCommit commit;
                try { rate.ensure(); commit = repo.getCommit(sha); }
                catch (Exception e) { continue; }

                String cur = fetchRawAtCommit(token, repoFull, sha, filePath);
                if (cur == null) continue;

                int lineIdx = parseIntSafe(rec.get("Line")) - 1;
                String methodSig;
                CodeRange curMethod;
                if (lineIdx >= 0) {
                    curMethod = findEnclosingMethod(cur, lineIdx);
                } else {
                    int firstHit = findFirstSignalLine(cur);
                    curMethod = findEnclosingMethod(cur, Math.max(0, firstHit));
                }
                if (curMethod == null) {
                    curMethod = new CodeRange(0, cur.split("\\R").length - 1);
                }
                methodSig = extractMethodSignature(cur, curMethod.start);

                String parentSha = firstParentSHA(commit);
                String parentUrl = parentSha.isEmpty() ? "" : "https://github.com/" + repoFull + "/commit/" + parentSha;

                String classification = "first-introduction";
                String originalCode = "";
                String refactoredCode = extractSnippet(cur, curMethod, signal, CONTEXT);
                String category = categorizeSignal(signal);
                String rationale = "Method or file not present in parent; or concurrency introduced at initial method addition.";

                if (!parentSha.isEmpty()) {
                    String prev = fetchRawAtCommit(token, repoFull, parentSha, filePath);
                    if (prev != null) {
                        CodeRange prevMethod = matchMethodInParent(prev, methodSig);
                        if (prevMethod != null) {
                            boolean parentHadSignals = hasAnySignal(prev, prevMethod);
                            boolean currentHasSignals = hasAnySignal(cur, curMethod);

                            if (!parentHadSignals && currentHasSignals) {
                                classification = "refactor";
                                originalCode = extractOriginalCandidate(prev, prevMethod);
                                refactoredCode = extractSnippet(cur, curMethod, signal, CONTEXT);
                                rationale = "Parent method existed without concurrency/streams; current commit introduces them.";
                            } else {
                                classification = "first-introduction";
                                rationale = "Parent method existed but already had similar constructs or none were introduced.";
                                originalCode = "";
                            }
                        } else {
                            classification = "first-introduction";
                            rationale = "Method absent in parent; introduced in this commit with the construct.";
                        }
                    } else {
                        classification = "first-introduction";
                        rationale = "File absent in parent; introduced in this commit.";
                    }
                }

                String commitUrl = "https://github.com/" + repoFull + "/commit/" + sha;
                String commitDateUtc = commitDateUtc(commit);

                out.printRecord(
                        repoFull, repoUrl,
                        sha, commitUrl, commitDateUtc,
                        parentSha, parentUrl,
                        filePath, methodSig, classification,
                        signal, category,
                        truncate(originalCode, MAX_SNIPPET),
                        truncate(refactoredCode, MAX_SNIPPET),
                        rationale
                );
                out.flush();
            }
        }

        System.out.println("✅ Wrote: " + OUTPUT_CSV);
    }

    // ===== CSV helpers =====
    private static String safe(String s){ return s==null? "": s.trim(); }
    private static String safeOr(String s){ return s==null? "": s.trim(); }
    private static int parseIntSafe(String s){ try { return Integer.parseInt(s.trim()); } catch(Exception e){ return -1; } }
    private static String repDeriveUrl(String full, String a, String b){
        if (a!=null && !a.isBlank()) return a;
        if (b!=null && !b.isBlank()) return b;
        if (full==null || full.isBlank()) return "";
        return "https://github.com/" + full;
    }

    private static class CodeRange { final int start, end; CodeRange(int s,int e){ start=s; end=e; } }

    private static CodeRange findEnclosingMethod(String content, int lineIdx) {
        String[] lines = content.split("\\R");
        int sigLine = -1;
        for (int i = Math.min(lineIdx, lines.length-1); i >= 0; i--) {
            if (METHOD_SIG.matcher(lines[i]).find()) { sigLine = i; break; }
        }
        if (sigLine < 0) return null;

        int depth = 0;
        int end = -1;
        for (int i = sigLine; i < lines.length; i++) {
            String ln = stripLineComments(lines[i]);
            for (int k=0;k<ln.length();k++){
                char c = ln.charAt(k);
                if (c=='{') depth++;
                else if (c=='}') { depth--; if (depth==0){ end=i; break; } }
            }
            if (end>=0) break;
        }
        if (end < sigLine) end = Math.min(sigLine+50, lines.length-1); // fallback
        return new CodeRange(sigLine, end);
    }

    private static String extractMethodSignature(String content, int sigLine) {
        if (sigLine<0) return "";
        String[] lines = content.split("\\R");
        if (sigLine>=lines.length) return "";
        Matcher m = METHOD_SIG.matcher(lines[sigLine]);
        if (!m.find()) return "";
        String name = m.group(1);
        String params = m.group(2);
        int paramCount = params==null || params.isBlank()? 0 : params.split(",").length;
        return name + "(" + paramCount + ")";
    }

    private static CodeRange matchMethodInParent(String parentContent, String methodSig) {
        if (methodSig==null || methodSig.isBlank()) return null;
        String name = methodSig.replaceAll("\\(.*\\)$", "");
        int desiredCount = 0;
        Matcher c = Pattern.compile("\\((\\d+)\\)$").matcher(methodSig);
        if (c.find()) desiredCount = Integer.parseInt(c.group(1));

        String[] lines = parentContent.split("\\R");
        for (int i=0;i<lines.length;i++){
            Matcher m = METHOD_SIG.matcher(lines[i]);
            if (m.find()){
                String n = m.group(1);
                String p = m.group(2);
                int cnt = p==null || p.isBlank()? 0 : p.split(",").length;
                if (n.equals(name) && cnt==desiredCount) {
                    CodeRange r = findEnclosingMethod(parentContent, i);
                    if (r!=null) return r;
                }
            }
        }
        for (int i=0;i<lines.length;i++){
            Matcher m = METHOD_SIG.matcher(lines[i]);
            if (m.find()){
                String n = m.group(1);
                if (n.equals(name)) {
                    CodeRange r = findEnclosingMethod(parentContent, i);
                    if (r!=null) return r;
                }
            }
        }
        return null;
    }

    private static boolean hasAnySignal(String content, CodeRange r) {
        String[] lines = content.split("\\R");
        for (int i=Math.max(0,r.start); i<=Math.min(lines.length-1,r.end); i++){
            String t = lines[i].trim();
            if (t.startsWith("import ") || t.startsWith("//") || t.startsWith("/*") || t.startsWith("*")) continue;
            for (String lit: DEFINITIVE_LITERALS) if (t.contains(lit)) return true;
            for (Pattern p: DEFINITIVE_REGEX) if (p.matcher(t).find()) return true;
        }
        return false;
    }

    private static String extractOriginalCandidate(String parent, CodeRange r) {
        String[] lines = parent.split("\\R");
        for (int i=r.start;i<=r.end;i++){
            String t = lines[i].trim();
            if (SEQ_LOOP_HINT.matcher(t).find() || FOREACH_NO_PAR_HINT.matcher(t).find()) {
                return extractSnippet(parent, around(i), "", CONTEXT);
            }
        }
        return extractSnippet(parent, r, "", CONTEXT);
    }

    private static CodeRange around(int center){
        return new CodeRange(Math.max(0, center - CONTEXT), center + CONTEXT);
    }

    private static String extractSnippet(String content, CodeRange r, String signal, int ctx) {
        String[] lines = content.split("\\R");
        int start = Math.max(0, r.start - 0);
        int end   = Math.min(lines.length-1, r.end + 0);

        int hit = -1;
        for (int i=start;i<=end;i++){
            String t = lines[i].trim();
            if (!t.startsWith("import ") && !t.startsWith("//") && !t.startsWith("/*") && !t.startsWith("*")) {
                if (!signal.isBlank() && t.contains(signal)) { hit = i; break; }
                for (Pattern p: DEFINITIVE_REGEX) { if (p.matcher(t).find()) { hit = i; break; } }
                if (hit>=0) break;
            }
        }
        if (hit>=0){
            int s = Math.max(start, hit - ctx);
            int e = Math.min(end,   hit + ctx);
            return formatBlock(lines, s, e, hit);
        }
        int s = start;
        int e = Math.min(end, start + 2*ctx);
        return formatBlock(lines, s, e, s);
    }

    private static String formatBlock(String[] lines, int start, int end, int focus) {
        StringBuilder sb = new StringBuilder();
        for (int i=start;i<=end;i++){
            String prefix = (i==focus) ? ">> " : "   ";
            sb.append(prefix).append(i+1).append(": ")
                    .append(lines[i].replace("\t","    "))
                    .append("\n");
        }
        String s = sb.toString().trim();
        return s.length()>MAX_SNIPPET ? s.substring(0, MAX_SNIPPET-3) + "..." : s;
    }

    // ===== Commit & HTTP helpers =====
    private static String firstParentSHA(GHCommit commit) {
        try {
            List<GHCommit> parents = commit.getParents();
            if (parents != null && !parents.isEmpty()) {
                String p = parents.get(0).getSHA1();
                return p == null ? "" : p;
            }
        } catch (Exception ignored) {}
        return "";
    }

    private static String commitDateUtc(GHCommit commit) {
        try {
            GHCommit.ShortInfo info = commit.getCommitShortInfo();
            Date when = null;
            if (info != null) {
                if (info.getAuthor() != null) when = info.getAuthor().getDate();
                else if (info.getCommitter() != null) when = info.getCommitter().getDate();
            }
            return when == null ? "" : when.toInstant().atZone(ZoneOffset.UTC).toString();
        } catch (Exception e){ return ""; }
    }

    private static String fetchRawAtCommit(String token, String fullName, String sha, String path) {
        try {
            String rawUrl = "https://raw.githubusercontent.com/" + fullName + "/" + sha + "/" + path;
            HttpURLConnection conn = (HttpURLConnection) new URL(rawUrl).openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "token " + token);
            conn.setRequestProperty("User-Agent", "IntroVsRefactorClassifier");
            int code = conn.getResponseCode();
            if (code == 200) return new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception ignored) {}
        return null;
    }

    private static String truncate(String s, int n) {
        if (s==null) return "";
        s = s.trim();
        return s.length() <= n ? s : s.substring(0, n - 3) + "...";
    }

    private static String stripLineComments(String s){
        int i = s.indexOf("//");
        return i>=0 ? s.substring(0,i) : s;
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

    private static class RateLimiter {
        private final GitHub gh;
        RateLimiter(GitHub gh){ this.gh = gh; }
        void ensure() throws IOException {
            GHRateLimit r = gh.getRateLimit();
            if (r.getRemaining() < 10) {
                long waitMs = Math.max(0, r.getResetDate().getTime() - System.currentTimeMillis() + 1500);
                System.out.printf("• Rate low; sleeping %,d ms…%n", waitMs);
                try { Thread.sleep(waitMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
    }

    private static int findFirstSignalLine(String content) {
        if (content == null || content.isEmpty()) return -1;

        String noBlock = content.replaceAll("(?s)/\\*.*?\\*/", "");
        String[] lines = noBlock.split("\\R");

        for (int i = 0; i < lines.length; i++) {
            String line = stripLineComments(lines[i]).trim();
            if (line.isEmpty()) continue;
            if (line.startsWith("import ") || line.startsWith("*")) continue;

            // Literal hits
            for (String lit : DEFINITIVE_LITERALS) {
                if (line.contains(lit)) return i;
            }
            // Regex hits
            for (Pattern p : DEFINITIVE_REGEX) {
                if (p.matcher(line).find()) return i;
            }
        }
        return -1;
    }
}

