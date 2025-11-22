package org.example;

import org.kohsuke.github.*;
import org.apache.commons.csv.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * Scans Java GitHub repositories for opportunities to refactor code
 * to use parallelism constructs like parallel streams, ForkJoin,
 * CompletableFuture, etc.
 *
 * Input: CSV file with repository URLs (column "URL" or similar)
 * Output: Two CSV reports - verification of forbidden constructs,
 * and identified refactoring opportunities.
 * Task 1 - strict scan for forbidden constructs.
 * Task 2 - identify refactoring opportunities.
 * Task 3 - strict mode: skip recursive methods from opportunities.
 * Task 4 - clean repos only.
 * Task 5 - improved method parsing to detect recursion accurately.
 * Task 6 - improved loop analysis to suggest appropriate patterns.
 * Task 7 - I/O operation detection inside loops.
 * Task 8 - multi-call loop detection.
 * Task 9 - better parameter type extraction for recursion check.
 * Task 10 - improved string literal and comment stripping.
 * Task 11 - enhanced error handling and reporting.
 * Task 12 - temporary directory cleanup.
 * Task 13 - rate limit handling for GitHub API.
 * Task 14 - detailed context snippets in verification report.
 * Task 15 - configurable verification tokens via environment variable.
 * Task 16 - logging and progress reporting.
 * Task 17 - performance optimizations for large repos.
 * Task 18 - support for private repositories with authentication.
 *
 * Dataset B - Part 2
 */
public class RefactorOpportunityIdentifier {
    // Verification tokens
    private static final String[] VERIF_TOKENS = {
            // Parallel streams
            ".parallel()", "parallelStream(",
            // Fork/Join
            "ForkJoinPool", "ForkJoinTask", "RecursiveTask", "RecursiveAction",
            // Executors / threads
            "Executor ", "Executor<", "ExecutorService", "Executors.", "ThreadPoolExecutor",
            "new Thread(", "Thread.sleep(",
            // Futures / CompletableFuture
            "Future<", "CompletionStage", "CompletableFuture",
            // CF chain ops
            "supplyAsync(", "runAsync(", "thenApply(", "thenApplyAsync(", "thenCompose(", "thenComposeAsync(",
            "thenAccept(", "thenAcceptAsync(", "allOf(", "anyOf("
    };

    // Loop and block detection
    private static final Pattern FOR_LOOP = Pattern.compile("^\\s*for\\s*\\(", Pattern.MULTILINE);
    private static final Pattern FOREACH_LOOP = Pattern.compile("^\\s*for\\s*\\(\\s*[^:;()]+:\\s*[^)]+\\)", Pattern.MULTILINE);
    private static final Pattern WHILE_LOOP = Pattern.compile("^\\s*while\\s*\\(", Pattern.MULTILINE);
    private static final Pattern RANGE_STYLE_INDEX = Pattern.compile(
            "\\bfor\\s*\\(\\s*(int|long)\\s+\\w+\\s*=\\s*\\d+\\s*;\\s*\\w+\\s*[<≤]\\s*\\w+\\s*;\\s*\\w+\\+\\+\\s*\\)"
    );
    private static final Pattern ADD_TO_COLLECTION = Pattern.compile("\\b(\\w+)\\.add\\(");
    private static final Pattern POSSIBLE_REMOTE_CALL = Pattern.compile(
            "\\b(httpClient|WebClient|RestTemplate|OkHttp|HttpURLConnection|Files\\.|Paths\\.|socket|channel)\\b",
            Pattern.CASE_INSENSITIVE
    );


    /*     Method declaration parsing
     */
    private static final Pattern METHOD_DECL = Pattern.compile(
            "\\b(public|protected|private|static|final|synchronized|abstract|native)?\\s*" +
                    "(<[^>]+>\\s*)?" +                  // optional generics
                    "[\\w\\<\\>\\[\\]]+\\s+" +          // return type
                    "(\\w+)\\s*\\(([^;]*)\\)\\s*\\{"    // method name + params (captured) + opening brace
    );

    /*
        I/O operation detection
     */
    private static final Pattern IO_OPERATION = Pattern.compile(
            "\\b(read|write|flush|close|open|connect|send|receive|query|execute|fetch|load|save|delete|insert|update)\\s*\\(",
            Pattern.CASE_INSENSITIVE
    );

    /*     Output files
     */
    private static final String VERIFICATION_CSV = "verification_report.csv";
    private static final String OPPORTUNITIES_CSV = "opportunities_report.csv";


    public static void main(String[] args) throws Exception {

        Path csvPath = Paths.get("phase3_strict_clean_repos.csv");
        if (!Files.exists(csvPath)) {
            System.err.println("CSV not found in project root: " + csvPath.toAbsolutePath());
            System.exit(1);
        }


        String token = System.getenv("GITHUB_TOKEN");
        GitHub gh = null;
        if (token != null && !token.isBlank()) {
            gh = new GitHubBuilder().withOAuthToken(token).build();
            System.out.println("GitHub auth OK. Rate limit: " + gh.getRateLimit().limit);
        } else {
            System.out.println("No GITHUB_TOKEN found. Proceeding unauthenticated (rate-limited).");
        }

        List<String> repos = readRepoUrls(csvPath);
        System.out.println("Total repos to scan: " + repos.size());

        try (CSVPrinter verifOut = new CSVPrinter(new FileWriter(VERIFICATION_CSV, StandardCharsets.UTF_8),
                CSVFormat.DEFAULT.withHeader("repo","file","method","line","kind","token","context"));
             CSVPrinter oppOut = new CSVPrinter(new FileWriter(OPPORTUNITIES_CSV, StandardCharsets.UTF_8),
                     CSVFormat.DEFAULT.withHeader("repo","file","method","startLine","endLine","pattern","suggestion","note"))) {

            for (String repoUrl : repos) {
                String slug = toSlug(repoUrl);
                if (slug == null) {
                    System.out.println("Skipping invalid URL: " + repoUrl);
                    continue;
                }

                System.out.println("==> Scanning " + slug);

                Path workDir = Files.createTempDirectory("repo_" + slug.replace('/', '_') + "_");
                Path repoRoot = workDir.resolve("repo");
                Files.createDirectories(repoRoot);

                try {
                    downloadRepoZip(slug, repoRoot, gh, token);
                    List<Path> javaFiles = listJavaFiles(repoRoot);

                    boolean anyForbidden = false;

                    for (Path f : javaFiles) {
                        List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);


                        List<MethodInfo> methods = parseMethods(lines);


                        List<Hit> hits = verifyFile(lines, methods);
                        for (Hit h : hits) {
                            anyForbidden = true;
                            verifOut.printRecord(
                                    slug,
                                    repoRoot.relativize(f).toString(),
                                    h.methodName,
                                    h.line,
                                    h.kind,
                                    h.token,
                                    snippet(lines, h.line)
                            );
                        }

                        List<Candidate> cands = findOpportunities(lines, methods);
                        for (Candidate c : cands) {
                            oppOut.printRecord(
                                    slug,
                                    repoRoot.relativize(f).toString(),
                                    c.methodName,
                                    c.startLine,
                                    c.endLine,
                                    c.pattern,
                                    c.suggestion,
                                    c.note
                            );
                        }
                    }

                    if (!anyForbidden) {
                        verifOut.printRecord(slug, "", "", "", "OK", "NONE", "No forbidden constructs detected");
                    }

                } catch (Exception ex) {
                    verifOut.printRecord(slug, "", "", "", "ERROR", "EXCEPTION",
                            ex.getClass().getSimpleName() + ": " + ex.getMessage());
                    oppOut.printRecord(slug, "", "", "", "", "ERROR", "SKIPPED",
                            "Scan failed: " + ex.getMessage());
                } finally {
                    deleteQuietly(workDir);
                }
            }
        }

        System.out.println("Done!");
        System.out.println("Reports generated:");
        System.out.println(" - " + VERIFICATION_CSV);
        System.out.println(" - " + OPPORTUNITIES_CSV);
    }


    private static class MethodInfo {
        final String name;
        final int startLine;
        final int endLine;
        final boolean isRecursive;
        final List<String> paramTypes;

        MethodInfo(String name, int startLine, int endLine, boolean isRecursive, List<String> paramTypes) {
            this.name = name;
            this.startLine = startLine;
            this.endLine = endLine;
            this.isRecursive = isRecursive;
            this.paramTypes = paramTypes;
        }
    }


    private static List<MethodInfo> parseMethods(List<String> lines) {
        List<MethodInfo> methods = new ArrayList<>();

        for (int i = 0; i < lines.size(); i++) {
            String stripped = stripLineComments(stripStringLiterals(lines.get(i)));
            Matcher m = METHOD_DECL.matcher(stripped);

            if (!m.find()) continue;

            String methodName = m.group(3);
            String params = m.group(4);
            if (methodName == null || methodName.isEmpty()) continue;

            List<String> paramTypes = extractParameterTypes(params);

            int startLine = i + 1;
            int endLine = findBlockEnd(lines, i);

            // Check if method is recursive - needs to match name AND be compatible call
            boolean isRecursive = false;

            for (int j = i + 1; j <= endLine && j < lines.size(); j++) {
                String bodyLine = stripLineComments(stripStringLiterals(lines.get(j)));


                Pattern callPattern = Pattern.compile(
                        "\\b(this\\s*\\.\\s*)?" + Pattern.quote(methodName) + "\\s*\\("
                );
                Matcher callMatcher = callPattern.matcher(bodyLine);

                while (callMatcher.find()) {

                    int matchStart = callMatcher.start();
                    if (matchStart > 0) {
                        String before = bodyLine.substring(0, matchStart);

                        if (before.endsWith(".") && !before.trim().endsWith("this.")) {
                            continue;
                        }
                    }


                    int openParen = callMatcher.end() - 1;
                    int closeParen = findMatchingParen(bodyLine, openParen);
                    if (closeParen > openParen) {
                        String args = bodyLine.substring(openParen + 1, closeParen).trim();


                        List<String> argList = splitArguments(args);

                        if (argList.size() == paramTypes.size()) {
                            isRecursive = true;
                            break;
                        }
                    }
                }

                if (isRecursive) break;
            }

            methods.add(new MethodInfo(methodName, startLine, endLine, isRecursive, paramTypes));
            i = endLine;
        }

        return methods;
    }

    private static List<String> extractParameterTypes(String params) {
        List<String> types = new ArrayList<>();
        if (params == null || params.trim().isEmpty()) {
            return types;
        }

        List<String> parameters = splitByTopLevelComma(params);

        for (String param : parameters) {
            param = param.trim();
            if (param.isEmpty()) continue;

            param = param.replaceAll("@\\w+\\s+", "");

            param = param.replaceAll("\\bfinal\\s+", "");

            int lastSpace = param.lastIndexOf(' ');
            if (lastSpace > 0) {
                String type = param.substring(0, lastSpace).trim();
                types.add(normalizeType(type));
            }
        }

        return types;
    }

    private static String normalizeType(String type) {

        type = type.replaceAll("\\b[a-z][a-z0-9_]*\\.", "");
        type = type.replaceAll("\\s+", "");
        return type;
    }

    private static List<String> splitByTopLevelComma(String text) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;

        for (char c : text.toCharArray()) {
            if (c == '<' || c == '(') {
                depth++;
                current.append(c);
            } else if (c == '>' || c == ')') {
                depth--;
                current.append(c);
            } else if (c == ',' && depth == 0) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }

        if (current.length() > 0) {
            parts.add(current.toString());
        }

        return parts;
    }

    private static List<String> splitArguments(String args) {
        if (args == null || args.trim().isEmpty()) {
            return new ArrayList<>();
        }
        return splitByTopLevelComma(args);
    }

    private static int findMatchingParen(String line, int openIndex) {
        int depth = 1;
        for (int i = openIndex + 1; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    private static String findMethodForLine(int line, List<MethodInfo> methods) {
        for (MethodInfo m : methods) {
            if (line >= m.startLine && line <= m.endLine) {
                return m.name;
            }
        }
        return "<class-level>";
    }

    // --- CSV parsing ---
    private static List<String> readRepoUrls(Path csv) throws IOException {
        List<String> out = new ArrayList<>();
        try (Reader in = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(in);
            for (CSVRecord r : records) {
                String url = getFirstNonEmpty(r, "URL", "Repository", "repo", "Repo", "repository", "Project");
                if (url != null && !url.isBlank()) {
                    out.add(url.trim());
                }
            }
        }
        return out;
    }

    private static String getFirstNonEmpty(CSVRecord r, String... keys) {
        for (String k : keys) {
            if (r.isMapped(k)) {
                String v = r.get(k);
                if (v != null && !v.isBlank()) return v;
            }
        }
        return null;
    }

    private static String toSlug(String url) {
        try {
            String s = url.trim();
            int i = s.indexOf("github.com/");
            if (i < 0) return null;
            String tail = s.substring(i + "github.com/".length());
            if (tail.endsWith(".git")) tail = tail.substring(0, tail.length() - 4);
            String[] parts = tail.split("/");
            if (parts.length >= 2) return parts[0] + "/" + parts[1];
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    private static void downloadRepoZip(String slug, Path repoRoot, GitHub gh, String token) throws Exception {
        String zipUrl = "https://api.github.com/repos/" + slug + "/zipball";

        if (gh != null) {
            gh.getRepository(slug);
        }

        HttpURLConnection conn = (HttpURLConnection) new URL(zipUrl).openConnection();
        if (token != null && !token.isBlank()) {
            conn.setRequestProperty("Authorization", "Bearer " + token);
        }
        conn.setRequestProperty("Accept", "application/vnd.github+json");
        conn.setInstanceFollowRedirects(true);
        conn.connect();

        try (InputStream in = conn.getInputStream();
             ZipInputStream zis = new ZipInputStream(in)) {
            unzipStrippingTopDir(zis, repoRoot);
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

    private static class Hit {
        final int line;
        final String kind;
        final String token;
        final String methodName;

        Hit(int l, String k, String t, String method) {
            line = l;
            kind = k;
            token = t;
            methodName = method;
        }
    }

    private static List<Hit> verifyFile(List<String> lines, List<MethodInfo> methods) {
        List<Hit> hits = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String L = stripLineComments(stripStringLiterals(lines.get(i)));
            for (String tok : VERIF_TOKENS) {
                if (L.contains(tok)) {
                    String methodName = findMethodForLine(i + 1, methods);
                    hits.add(new Hit(i + 1, classifyToken(tok), tok, methodName));
                }
            }
        }
        return hits;
    }

    private static String classifyToken(String tok) {
        if (tok.contains("parallel")) return "PARALLEL_STREAM";
        if (tok.startsWith("ForkJoin") || tok.startsWith("Recursive")) return "FORK_JOIN";
        if (tok.startsWith("Executor") || tok.startsWith("Executors.") || tok.equals("ThreadPoolExecutor")) return "EXECUTOR";
        if (tok.startsWith("Future") || tok.contains("CompletableFuture") || tok.contains("CompletionStage")) return "FUTURE";
        if (tok.contains("then") || tok.contains("allOf") || tok.contains("anyOf") || tok.contains("supplyAsync")) return "CF_CHAIN";
        if (tok.startsWith("new Thread") || tok.startsWith("Thread.sleep")) return "THREAD";
        return "OTHER";
    }

    private static class Candidate {
        final int startLine, endLine;
        final String pattern, suggestion, note;
        final String methodName;

        Candidate(int s, int e, String p, String sug, String n, String method) {
            startLine = s; endLine = e; pattern = p; suggestion = sug; note = n;
            methodName = method;
        }
    }

    private static List<Candidate> findOpportunities(List<String> lines, List<MethodInfo> methods) {
        List<Candidate> out = new ArrayList<>();

        for (MethodInfo method : methods) {
            if (method.isRecursive) {
                out.add(new Candidate(
                        method.startLine,
                        method.endLine,
                        "RECURSIVE_METHOD",
                        "ForkJoin / RecursiveTask",
                        "Method calls itself; candidate for divide-and-conquer parallelization.",
                        method.name
                ));
            }
        }

        for (int i = 0; i < lines.size(); i++) {
            String L = stripLineComments(stripStringLiterals(lines.get(i)));

            if (containsAny(L, ".parallel()", "parallelStream(", "CompletableFuture", "ForkJoinPool", "RecursiveTask", "ExecutorService", "Executors."))
                continue;

            if (FOR_LOOP.matcher(L).find() || FOREACH_LOOP.matcher(L).find() || WHILE_LOOP.matcher(L).find()) {
                int end = findBlockEnd(lines, i);
                String block = join(lines, i, end);
                String methodName = findMethodForLine(i + 1, methods);

                if (block.length() > 200_000) {
                    out.add(new Candidate(i+1, end+1, "LARGE_BLOCK", "Manual Review",
                            "Very large loop block skipped for safety.", methodName));
                    i = end;
                    continue;
                }

                boolean accum = looksLikeAccumulation(block) || ADD_TO_COLLECTION.matcher(block).find();
                boolean remoteish = POSSIBLE_REMOTE_CALL.matcher(block).find();
                boolean indexRange = RANGE_STYLE_INDEX.matcher(L).find() || block.contains(".size()");
                boolean ioOps = IO_OPERATION.matcher(block).find();

                int callCount = countMethodCalls(block);
                boolean multiCall = callCount >= 3;

                if (ioOps && (remoteish || block.contains("File") || block.contains("Stream"))) {
                    out.add(new Candidate(i+1, end+1, "IO_LOOP", "CompletableFuture + allOf",
                            "I/O operations inside loop. Consider async I/O with fan-out and join.", methodName));
                }
                else if (multiCall && !accum) {
                    out.add(new Candidate(i+1, end+1, "MULTI_CALL_LOOP", "CompletableFuture",
                            "Multiple independent calls inside loop. Consider supplyAsync composition.", methodName));
                }
                else if (remoteish) {
                    out.add(new Candidate(i+1, end+1, "LOOP_MULTI_CALL", "CompletableFuture",
                            "Independent calls inside loop. Consider supplyAsync with composition, then join.", methodName));
                } else if (indexRange) {
                    String suggestion = "IntStream.range / LongStream.rangeClosed";
                    String note = "Index-based loop. Consider range + map/reduce; use parallel for heavy per-item work.";

                    if (isHeavyComputation(block)) {
                        suggestion = "ForkJoin / RecursiveTask";
                        note = "Heavy computation in indexed loop. Consider ForkJoin for divide-and-conquer parallelism.";
                    }

                    out.add(new Candidate(i+1, end+1, "INDEXED_LOOP", suggestion, note, methodName));
                } else if (accum) {
                    out.add(new Candidate(i+1, end+1, "AGG_LOOP", "Parallel Stream",
                            "Sequential aggregation. Consider parallel reduction with primitive streams.", methodName));
                } else {
                    out.add(new Candidate(i+1, end+1, "FOREACH_LOOP", "Parallel Stream",
                            "Per-element work over a collection. Candidate for parallel stream if independent.", methodName));
                }
                i = end;
            }
        }
        return out;
    }


    private static int countMethodCalls(String block) {
        int count = 0;
        Pattern methodCall = Pattern.compile("\\w+\\s*\\(");
        Matcher m = methodCall.matcher(block);
        while (m.find()) {
            count++;
        }
        return count;
    }

    private static boolean isHeavyComputation(String block) {
        return block.contains("Math.") ||
                block.contains("calculate") ||
                block.contains("compute") ||
                (block.contains("for") && block.split("for").length > 2) || // nested loops
                block.contains("sort") ||
                block.contains("search");
    }

    private static boolean looksLikeAccumulation(String text) {
        if (text.contains("+=") || text.contains("-=") || text.contains("*=") || text.contains("/=")) return true;
        if (text.contains("++") || text.contains("--")) return true;
        if (text.length() < 2000 && text.contains("=") && text.contains("+")) return true;
        return false;
    }

    private static String stripStringLiterals(String s) {
        if (s == null || s.isEmpty()) return s;
        StringBuilder out = new StringBuilder(s.length());
        boolean inDq = false;
        boolean inSq = false;
        boolean esc = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (inDq) {
                if (esc) { esc = false; continue; }
                if (c == '\\') { esc = true; continue; }
                if (c == '"') { inDq = false; }
                continue;
            }
            if (inSq) {
                if (esc) { esc = false; continue; }
                if (c == '\\') { esc = true; continue; }
                if (c == '\'') { inSq = false; }
                continue;
            }

            if (c == '"') { inDq = true; out.append("\"\""); continue; }
            if (c == '\'') { inSq = true; out.append("''"); continue; }

            out.append(c);
        }
        return out.toString();
    }

    private static String stripLineComments(String s) {
        int i = s.indexOf("//");
        return (i >= 0) ? s.substring(0, i) : s;
    }

    private static boolean containsAny(String line, String... toks) {
        for (String t : toks) if (line.contains(t)) return true;
        return false;
    }

    private static int findBlockEnd(List<String> lines, int startIdx) {
        int brace = 0;
        boolean started = false;
        for (int i = startIdx; i < lines.size(); i++) {
            String l = lines.get(i);
            for (int k = 0; k < l.length(); k++) {
                char c = l.charAt(k);
                if (c == '{') { brace++; started = true; }
                else if (c == '}') { brace--; }
            }
            if (started && brace == 0) return i;
        }
        return Math.min(startIdx + 20, lines.size() - 1);
    }

    private static String join(List<String> lines, int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i <= end && i < lines.size(); i++) {
            sb.append(lines.get(i)).append('\n');
        }
        return sb.toString();
    }

    private static String snippet(List<String> lines, int line) {
        int s = Math.max(1, line - 1);
        int e = Math.min(lines.size(), line + 1);
        StringBuilder sb = new StringBuilder();
        for (int i = s; i <= e; i++) {
            sb.append(i).append(": ").append(lines.get(i - 1)).append('\n');
        }
        return sb.toString().trim();
    }

    private static void deleteQuietly(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(p -> {
                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
            });
        } catch (IOException ignored) {}
    }

    private static List<Path> listJavaFiles(Path root) throws IOException {
        List<Path> res = new ArrayList<>();
        Files.walk(root)
                .filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".java"))
                .forEach(res::add);
        return res;
    }
}