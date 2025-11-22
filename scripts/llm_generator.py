#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

# ========= Paths / layout =========
SCRIPTS_DIR = Path(__file__).resolve().parent
ROOT_DIR    = SCRIPTS_DIR.parent
JAVA_DIR    = ROOT_DIR / "java"

# lib/ or libs/ jars (javaparser, gson)
for _cand in (ROOT_DIR / "libs", ROOT_DIR / "lib"):
    if _cand.exists():
        LIB_DIR = _cand
        break
else:
    LIB_DIR = ROOT_DIR / "libs"

TEST_OUT  = ROOT_DIR / "test_out"
DEBUG_DIR = TEST_OUT / "debug"
OUT_JSON  = TEST_OUT / "concurrency_suggestions.json"

AST_SOURCE = SCRIPTS_DIR / "GenerateAST.java"
AST_CLASS  = "scripts.GenerateAST"  # must match package/class in GenerateAST.java

# ========= LLM settings =========
DEFAULT_MODEL         =  "qwen2.5-coder:7b-instruct"    #"llama3 (less accurate suggestions), codellama:13b-instruct (too large for memory), qwen2.5-coder:7b-instruct (optimal for 16bg laptop running locally)"
DEFAULT_STRICT        = True
MAX_METHODS_PER_CHUNK = 1       # Keep at 1
LLM_TIMEOUT_SEC       = 300.0   # Keep at 300.0
OLLAMA_HOST           = "127.0.0.1:11434"

# ========= Log helpers =========
def info(msg: str) -> None: print(msg)
def warn(msg: str) -> None: print(msg)
def err(msg: str) -> None: print(msg, file=sys.stderr)

# ========= FS helpers =========
def ensure_dirs() -> None:
    TEST_OUT.mkdir(parents=True, exist_ok=True)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

def glob_jars() -> List[str]:
    if not LIB_DIR.exists(): return []
    return [str(p) for p in LIB_DIR.glob("*.jar")]

def shell_run(cmd: List[str], cwd: Optional[Path] = None, capture: bool = True) -> Tuple[int, str, str]:
    p = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
        text=True
    )
    out, err_ = p.communicate()
    return p.returncode, out or "", err_ or ""

# ========= AST compile & run =========
def compile_ast_generator() -> None:
    ensure_dirs()
    jars = glob_jars()
    if not jars:
        err(f"[ast] No .jar files found in {LIB_DIR.name}. Put javaparser-core*.jar and gson*.jar there.")
        sys.exit(1)
    cp = os.pathsep.join([str(TEST_OUT)] + jars)
    code, _, e = shell_run(["javac", "-cp", cp, "-d", str(TEST_OUT), str(AST_SOURCE)])
    if code != 0:
        err("[ast] Compile failed.\n" + e)
        sys.exit(1)
    info("[ast] Compile OK.")

def run_ast_generator(java_file: Path) -> Dict[str, Any]:
    jars = glob_jars()
    cp = os.pathsep.join([str(TEST_OUT), *jars])
    code, out, e = shell_run(["java", "-cp", cp, AST_CLASS, str(java_file)])
    if code != 0:
        err(f"[ast] Runner failed on {java_file.name}\n{e}")
        return {}
    try:
        return json.loads(out)
    except Exception as ex:
        warn(f"[ast] JSON parse error on {java_file.name}: {ex}")
        (DEBUG_DIR / f"{java_file.stem}.ast.raw.txt").write_text(out, encoding="utf-8")
        return {}

# ========= Mask/unmask =========
MASK_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")

def mask_code(code_text: str) -> Tuple[str, str, str, Dict[str, str]]:
    """
    Return masked code + reverse map (token->original) to unmask LLM output.
    """
    unmask_map_raw: Dict[str, str] = {}
    def repl(m):
        tok = m.group(0)
        if tok not in unmask_map_raw:
            unmask_map_raw[tok] = f"T_{len(unmask_map_raw)+1}"
        return unmask_map_raw[tok]

    masked = MASK_PATTERN.sub(repl, code_text or "")
    masked_sig = masked.splitlines()[0] if masked else ""
    masked_cls = "UnknownClass"
    m = re.search(r"class\s+([A-Za-z_][A-Za-z0-9_]*)", masked)
    if m:
        masked_cls = m.group(1)
    reverse_map: Dict[str, str] = {v: k for k, v in unmask_map_raw.items()}
    return masked, masked_sig, masked_cls, reverse_map

def unmask_code(text: str, unmask_map: Dict[str, str]) -> str:
    if not text: return text
    for k in sorted(unmask_map.keys(), key=len, reverse=True):
        text = text.replace(k, unmask_map[k])
    return text

def unmask_imports(imports: List[str], unmask_map: Dict[str, str]) -> List[str]:
    return [unmask_code(s, unmask_map) for s in imports]

# ========= Candidate construction from AST =========
def build_candidates_from_ast(ast: Dict[str, Any], source_text: str) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    # loops
    for lp in ast.get("loops", []):
        # The Java code provides 'loopBody', not 'body'.
        body = lp.get("loopBody", "") 
        start_line = lp.get("startLine", 0)
        ctx = lp.get("context") or {}
        masked_body, _, _, unmask_map = mask_code(body)
        candidates.append({
            "candidate_id": f"loop_{ctx.get('className','X')}_{ctx.get('methodSignature','M')}_{start_line}",
            "candidate_type": "loop",
            "start_line": start_line,
            "body_original": body,
            "body_masked": masked_body,
            "context": ctx,
            "unmask_map": unmask_map
        })

    # recursion
    for rm in ast.get("recursiveMethods", []):
        # The 'rm' object *is* the context, it's not nested.
        ctx = rm  
        
        # The Java code provides 'fullMethodBody', not 'body'.
        body = rm.get("fullMethodBody", "") 
        
        start_line = rm.get("startLine", 0)
        
        masked_body, _, _, unmask_map = mask_code(body)
        
        candidates.append({
            "candidate_id": f"rec_{ctx.get('className','X')}_{ctx.get('methodSignature','M')}_{start_line}",
            "candidate_type": "recursion",
            "start_line": start_line,
            "body_original": body,
            "body_masked": masked_body,
            "context": ctx,
            "unmask_map": unmask_map
        })

    return candidates

# ========= Fallback generator (safe, minimal) =========
def synthesize_guarded_fallback(candidate: Dict[str, Any]) -> Dict[str, Any]:
    ct = candidate.get("candidate_type", "loop")
    if ct == "loop":
        after = (
            "// Parallel guarded template\n"
            "int _N = collection != null ? collection.size() : 0;\n"
            "if (_N > 1000) {\n"
            "    java.util.stream.IntStream.range(0, _N).parallel().forEach(i -> {\n"
            "        // original body\n"
            "    });\n"
            "} else {\n"
            "    for (int i = 0; i < _N; i++) {\n"
            "        // original body\n"
            "    }\n"
            "}\n"
        )
        imports: List[str] = []
    else:
        after = (
            "// ForkJoin guarded template\n"
            "java.util.concurrent.ForkJoinPool pool = new java.util.concurrent.ForkJoinPool();\n"
            "try {\n"
            "    pool.submit(() -> {\n"
            "        // recursive tasks\n"
            "    }).join();\n"
            "} finally {\n"
            "    pool.shutdown();\n"
            "}\n"
        )
        imports = []

    return {
        "refactor_strategy": "Fallback",
        "imports": imports,
        "after_snippet": after,
        "explanation": "Guarded safe parallel template."
    }

# ========= Prompt slimming =========
_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE_RE  = re.compile(r"//.*?$", re.MULTILINE)
_MULTI_SPACE_RE   = re.compile(r"[ \t]{2,}")

def _slim_code(s: str, max_lines: int = 40, max_chars: int = 3500) -> str:
    """Remove comments & empty/brace-only lines, compress spaces, clip length."""
    if not s: return s
    s = _COMMENT_BLOCK_RE.sub("", s)
    s = _COMMENT_LINE_RE.sub("", s)
    lines_out: List[str] = []
    for line in s.splitlines():
        ln = line.strip()
        if not ln: continue
        if ln in {"{", "}"}: continue
        lines_out.append(ln)
        if len(lines_out) >= max_lines:
            break
    s = "\n".join(lines_out)
    s = _MULTI_SPACE_RE.sub(" ", s)
    return s[:max_chars]

# ========= Prompt builder (compact JSON) =========
def build_llm_prompt(chunk: List[Dict[str, Any]]) -> str:
    """
    Ask for STRICT JSON only. Send slimmed, masked bodies to keep prompt small.
    """
    schema_hint = {"suggestions":[{"candidate_id":"string","after_snippet":"string","imports":["string"],"refactor_strategy":"string","explanation":"string"}]}

    items = []
    for c in chunk:
        items.append({
            "candidate_id": c.get("candidate_id",""),
            "type": c.get("candidate_type",""),
            "class": c.get("context",{}).get("className",""),
            "method_signature": c.get("context",{}).get("methodSignature",""),
            "start_line": c.get("start_line",-1),
            "body_masked": _slim_code(c.get("body_masked",""), max_lines=40, max_chars=3500)
        })

    schema_str = json.dumps(schema_hint, separators=(",",":"))
    items_str  = json.dumps(items, separators=(",",":"))

    # <<< MODIFIED PROMPT (V6-SAFE) - 15 GENERAL RULES FOCUSED ON SAFETY >>>
    # This was the prompt that produced the fewest "unacceptable" (behavior-changing)
    # failures, as it correctly prioritized safety.
    prompt = (
        "You are an expert Java concurrency assistant. For each candidate, propose a safe, minimal refactor. "
        "Return STRICT JSON ONLY matching this schema:" + schema_str +
        " --- RULES ---"
        # --- SAFETY-FIRST PRIME DIRECTIVE ---
        "1.  **PRIME DIRECTIVE:** Your top priority is to **Preserve Behavior** and **Ensure Compilation**. If a parallel refactor risks violating these, you MUST suggest a simple sequential refactor or set `refactor_strategy: \"none\"` instead."
        
        "2.  **Guarded Paths:** Always use a guarded sequential path for small inputs (e.g., `if (collection.size() < 1000)`) to avoid parallel overhead."
        "3.  **For `type: \"loop\"` (Math/Filter):** For simple loops (math, filtering, mapping), **always prefer parallel streams** (e.g., `.parallelStream()`, `LongStream.range().parallel()`)."
        "4.  **For `type: \"loop\"` (Arrays):** For array operations, **always prefer built-in `java.util.Arrays` helpers** (e.g., `Arrays.parallelSort()`, `Arrays.parallelPrefix()`)."
        "5.  **For `type: \"loop\"` (I/O):** When a loop iterates to perform **blocking I/O (like reading files or network URLs), the refactor is `.parallelStream()` on the collection** or `CompletableFuture.supplyAsync` with a **custom Executor**."
        "6.  **Collectors:** When aggregating into a map, **prefer `Collectors.groupingByConcurrent()`**."
        
        "7.  **For `type: \"recursion\"`:** For recursive methods (like `fibRecursive`), the **correct refactor is a custom `RecursiveTask` using the `ForkJoin` framework**. This task **MUST** split the work (e.g., `n/2` or `n-1` and `n-2`) and make two recursive calls that are `fork()`ed and `join()`ed. A task that only makes **one** recursive call (e.g., `n-1`) is **NOT** parallel and is an **unacceptable suggestion**. If you cannot split the work, you MUST suggest `refactor_strategy: \"none\"`."
        
        "8.  **Snippet Format:** The `after_snippet` MUST contain the **entire refactored method**, from the signature to its closing brace `}`. **If a helper method (like a 'fetchUrl' method) or helper class (like a 'RecursiveTask') is needed, it MUST be included as a private method or private static class *inside* the `after_snippet`**, after the refactored method's closing brace `}`. If you forget the helper, it is a **compilation failure**."
        
        "9.  **Loop Boundaries:** Pay close attention to loop boundaries (e.g., `< n` vs. `<= n`) and preserve them."
        "10. **Race Conditions:** For `prefixSum`, `Arrays.parallelPrefix` is the only correct answer. A `parallel().forEach()` will cause a race condition and is wrong."
        
        # --- CRITICAL RULES (STRENGTHENED) ---
        "11. **CRITICAL (Code vs. Strategy):** If `refactor_strategy` involves parallelism (e.g., 'parallel stream'), the `after_snippet` code *must* contain a parallel call (`.parallel()`, `.parallelStream()`, `ForkJoinPool`, etc.). **Review your code to ensure you did not write `.stream()` by mistake. If you are not using a parallel call, you MUST set `refactor_strategy` to 'sequential stream' or 'none'.**"
        
        "12. **CRITICAL (Preserve Behavior):** The `after_snippet` MUST preserve the original method's logic. **DO NOT replace real work (like I/O) with a simulation (like `Thread.sleep`)**. Pay close attention to the *exact* logic (e.g., **counting lines** vs. **summing line lengths**, or **filtering files** vs. not filtering)."
        
        "13. **CRITICAL (No Algorithm Change):** Do not change the fundamental algorithm. For a candidate like `countPrimes`, you MUST parallelize the *calls* to `isPrime`. You **MUST NOT** implement a Sieve of Eratosthenes."
        
        "14. **No Needless Changes:** If a candidate is a small, efficient helper method (like `isPrime`) or an I/O loop that cannot be parallelized (like `copyFile`), the correct `refactor_strategy` is \"none\". **Do not change the algorithm of helper methods.**"
        "15. **FINAL CHECK:** Before outputting the JSON, review your `after_snippet`. Does it obey the **PRIME DIRECTIVE (Rule 1)**? Does it obey all CRITICAL rules (11, 12, 13)? Fix any mistakes."

        " --- CANDIDATES ---"
        + items_str +
        " Reply with a single JSON object containing key 'suggestions'."
    )
    # <<< END OF MODIFIED PROMPT >>>


    # Optional: auto-tighten if prompt too large (ensure new rules are copied here)
    if len(prompt) > 24000:
        items2 = []
        for c in chunk:
            items2.append({
                "candidate_id": c.get("candidate_id",""),
                "type": c.get("candidate_type",""),
                "class": c.get("context",{}).get("className",""),
                "method_signature": c.get("context",{}).get("methodSignature",""),
                "start_line": c.get("start_line",-1),
                "body_masked": _slim_code(c.get("body_masked",""), max_lines=25, max_chars=2200)
            })
        items_str = json.dumps(items2, separators=(",",":"))
        
        # (Make sure to update the fallback prompt as well)
        prompt = (
            "You are an expert Java concurrency assistant. For each candidate, propose a safe, minimal refactor. "
            "Return STRICT JSON ONLY matching this schema:" + schema_str +
            " --- RULES ---"
            # (All 15 rules would be copied here)
            "1.  **PRIME DIRECTIVE:** Your top priority is to **Preserve Behavior** and **Ensure Compilation**. If a parallel refactor risks violating these, you MUST suggest a simple sequential refactor or set `refactor_strategy: \"none\"` instead."
            "2.  **Guarded Paths:** Always use a guarded sequential path for small inputs (e.g., `if (collection.size() < 1000)`) to avoid parallel overhead."
            "3.  **For `type: \"loop\"` (Math/Filter):** For simple loops (math, filtering, mapping), **always prefer parallel streams** (e.g., `.parallelStream()`, `LongStream.range().parallel()`)."
            "4.  **For `type: \"loop\"` (Arrays):** For array operations, **always prefer built-in `java.util.Arrays` helpers** (e.g., `Arrays.parallelSort()`, `Arrays.parallelPrefix()`)."
            "5.  **For `type: \"loop\"` (I/O):** When a loop iterates to perform **blocking I/O (like reading files or network URLs), the refactor is `.parallelStream()` on the collection** or `CompletableFuture.supplyAsync` with a **custom Executor**."
            "6.  **Collectors:** When aggregating into a map, **prefer `Collectors.groupingByConcurrent()`**."
            "7.  **For `type: \"recursion\"`:** For recursive methods (like `fibRecursive`), the **correct refactor is a custom `RecursiveTask` using the `ForkJoin` framework**. This task **MUST** split the work (e.g., `n/2` or `n-1` and `n-2`) and make two recursive calls that are `fork()`ed and `join()`ed. A task that only makes **one** recursive call (e.g., `n-1`) is **NOT** parallel and is an **unacceptable suggestion**. If you cannot split the work, you MUST suggest `refactor_strategy: \"none\"`."
            "8.  **Snippet Format:** The `after_snippet` MUST contain the **entire refactored method**, from the signature to its closing brace `}`. **If a helper method (like a 'fetchUrl' method) or helper class (like a 'RecursiveTask') is needed, it MUST be included as a private method or private static class *inside* the `after_snippet`**, after the refactored method's closing brace `}`. If you forget the helper, it is a **compilation failure**."
            "9.  **Loop Boundaries:** Pay close attention to loop boundaries (e.g., `< n` vs. `<= n`) and preserve them."
            "10. **Race Conditions:** For `prefixSum`, `Arrays.parallelPrefix` is the only correct answer. A `parallel().forEach()` will cause a race condition and is wrong."
            "11. **CRITICAL (Code vs. Strategy):** If `refactor_strategy` involves parallelism (e.g., 'parallel stream'), the `after_snippet` code *must* contain a parallel call (`.parallel()`, `.parallelStream()`, `ForkJoinPool`, etc.). **Review your code to ensure you did not write `.stream()` by mistake. If you are not using a parallel call, you MUST set `refactor_strategy` to 'sequential stream' or 'none'.**"
            "12. **CRITICAL (Preserve Behavior):** The `after_snippet` MUST preserve the original method's logic. **DO NOT replace real work (like I/O) with a simulation (like `Thread.sleep`)**. Pay close attention to the *exact* logic (e.g., **counting lines** vs. **summing line lengths**, or **filtering files** vs. not filtering)."
            "13. **CRITICAL (No Algorithm Change):** Do not change the fundamental algorithm. For a candidate like `countPrimes`, you MUST parallelize the *calls* to `isPrime`. You **MUST NOT** implement a Sieve of Eratosthenes."
            "14. **No Needless Changes:** If a candidate is a small, efficient helper method (like `isPrime`) or an I/O loop that cannot be parallelized (like `copyFile`), the correct `refactor_strategy` is \"none\". **Do not change the algorithm of helper methods.**"
            "15. **FINAL CHECK:** Before outputting the JSON, review your `after_snippet`. Does it obey the **PRIME DIRECTIVE (Rule 1)**? Does it obey all CRITICAL rules (11, 12, 13)? Fix any mistakes."
            " --- CANDIDATES ---"
            + items_str +
            " Reply with a single JSON object containing key 'suggestions'."
        )
    return prompt

# ========= Ollama health, session, request =========
def ollama_is_ready() -> bool:
    try:
        import requests
        r = requests.get(f"http://{OLLAMA_HOST}/api/tags", timeout=5)
        return r.status_code == 200
    except Exception:
        return False

_session_holder: Dict[str, Any] = {"s": None}
def _get_session():
    try:
        import requests
    except Exception:
        err("[llm] Missing dependency: install 'requests' (pip install requests).")
        sys.exit(1)
    s = _session_holder.get("s")
    if s is None:
        s = requests.Session()
        s.headers.update({"Content-Type":"application/json"})
        _session_holder["s"] = s
    return s

def sleep_backoff(attempt: int) -> None:
    time.sleep(min(2 ** attempt, 8))

def run_llm_on_chunk(model: str, strict: bool, prompt: str) -> str:
    """
    POST to Ollama /api/generate with retries. Returns model's 'response' text or "" on failure.
    """
    if not ollama_is_ready():
        warn("[llm] Ollama server not ready. Using fallbacks for this chunk.")
        return ""

    s = _get_session()
    url = f"http://{OLLAMA_HOST}/api/generate"
    body: Dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "options": {
            "temperature": 0.0,
            "num_ctx": 2048,
            "num_predict": 2048
        },
        "keep_alive": "30m",
        "stream": False
    }
    if strict:
        body["format"] = "json"

    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            per_attempt_timeout = 120.0 if attempt < max_attempts - 1 else LLM_TIMEOUT_SEC
            r = s.post(url, json=body, timeout=per_attempt_timeout)
            r.raise_for_status()
            try:
                env = r.json()
                if isinstance(env, dict) and "response" in env:
                    return env["response"] or ""
                return r.text or ""
            except Exception:
                return r.text or ""
        except Exception as ex:
            warn(f"[llm] Attempt {attempt+1}/{max_attempts} failed: {ex}")
            if attempt < max_attempts - 1:
                sleep_backoff(attempt + 1)
                continue
            return ""

# ========= Robust LLM JSON handling =========
def process_llm_json_output(
    raw_output: str,
    chunk_candidates: List[Dict[str, Any]],
    unmask_map_unused: Dict[str, str]
) -> List[Dict[str, Any]]:
    """
    Try to parse LLM JSON, else fall back per-candidate. Merge valid suggestions only.
    """
    # Prepare per-candidate fallback results
    fallback_results: Dict[str, Dict[str, Any]] = {}
    for candidate in chunk_candidates:
        cid = candidate.get("candidate_id", "unknown_id")
        ctx  = candidate.get("context") or {}
        cls  = ctx.get("className", "UnknownClass")
        sig  = ctx.get("methodSignature", "UnknownSignature")

        fb = synthesize_guarded_fallback(candidate)
        fb["after_snippet"] = unmask_code(fb.get("after_snippet", ""), candidate.get("unmask_map", {}))

        if "className" not in ctx or "methodSignature" not in ctx:
            warn(f"[scan] Missing context fields for {cid}; using defaults.")

        fallback_results[cid] = {
            "candidate_id": cid,
            "class_name": cls,
            "method_signature": sig,
            "start_line": candidate.get("start_line", -1),
            "before_snippet": candidate.get("body_original", ""),
            **fb,
        }

    def _extract_json_block(txt: str) -> str:
        s = (txt or "").strip()
        if not s:
            return s
        # 0) object with "suggestions"
        if '"suggestions"' in s:
            m = re.search(r'\{.*"suggestions"\s*:\s*\[.*?\].*\}', s, re.DOTALL)
            if m:
                return m.group(0).strip()
        # 1) ```json ... ```
        m = re.search(r"```json\s*(.*?)```", s, re.DOTALL | re.IGNORECASE)
        if m and '"suggestions"' in m.group(1):
            return m.group(1).strip()
        # 2) ``` ... ```
        m = re.search(r"```\s*(.*?)```", s, re.DOTALL)
        if m and '"suggestions"' in m.group(1):
            return m.group(1).strip()
        # 3) largest brace block containing suggestions
        blocks = re.findall(r'\{.*?\}', s, re.DOTALL)
        blocks = [b for b in blocks if '"suggestions"' in b]
        if blocks:
            return max(blocks, key=len).strip()
        return s

    def _valid_suggestion(obj: Any) -> bool:
        return (isinstance(obj, dict) and 
                "candidate_id" in obj and 
                "after_snippet" in obj and 
                (("imports" not in obj) or isinstance(obj.get("imports"), list)))

    try:
        json_str = _extract_json_block(raw_output)
        data = json.loads(json_str)
        suggestions = data.get("suggestions")
        if not isinstance(suggestions, list):
            raise ValueError("Missing or non-list 'suggestions'")

        for sug in suggestions:
            if not _valid_suggestion(sug):
                continue
            cid = sug.get("candidate_id")
            if cid not in fallback_results:
                continue
            cand = next((c for c in chunk_candidates if c.get("candidate_id") == cid), None)
            per_map = cand.get("unmask_map", {}) if cand else {}
            fallback_results[cid].update({
                "refactor_strategy": sug.get("refactor_strategy", "LLM"),
                "imports": unmask_imports(sug.get("imports", []), per_map),
                "after_snippet": unmask_code(sug.get("after_snippet", ""), per_map),
                "explanation": sug.get("explanation", "—"),
            })
    except Exception as e:
        warn(f"  [llm] JSON parse failed: {e}. Returning fallbacks for all {len(chunk_candidates)} candidates.")
    return list(fallback_results.values())

# ========= Chunking & adaptive split =========
def chunk_candidates(all_candidates: List[Dict[str, Any]], max_per: int) -> List[List[Dict[str, Any]]]:
    return [all_candidates[i:i+max_per] for i in range(0, len(all_candidates), max_per)]

def process_chunk_recursively(
    chunk: List[Dict[str, Any]],
    model: str,
    strict: bool,
    chunk_index: int
) -> List[Dict[str, Any]]:
    prompt = build_llm_prompt(chunk)
    raw = run_llm_on_chunk(model, strict, prompt)

    # Save raw model output for debugging
    try:
        (DEBUG_DIR / f"chunk_{chunk_index:03d}.llm.txt").write_text(raw or "", encoding="utf-8")
    except Exception:
        pass

    combined_unmask: Dict[str, str] = {}
    for c in chunk:
        combined_unmask.update(c.get("unmask_map", {}))

    suggestions = process_llm_json_output(raw, chunk, combined_unmask)

    # If likely timeout (empty raw) produced only fallbacks, split and retry halves
    all_fallback = all(s.get("refactor_strategy") == "Fallback" for s in suggestions)
    if all_fallback and (not raw or not raw.strip()) and len(chunk) > 1:
        mid = len(chunk) // 2
        left = process_chunk_recursively(chunk[:mid], model, strict, chunk_index * 2 - 1)
        right = process_chunk_recursively(chunk[mid:], model, strict, chunk_index * 2)
        return left + right

    return suggestions

# ========= Main pipeline =========
def main() -> None:
    ensure_dirs()
    compile_ast_generator()

    java_files = sorted(JAVA_DIR.glob("**/*.java"))
    if not java_files:
        warn(f"[scan] No Java files found under {JAVA_DIR}.")
        OUT_JSON.write_text(json.dumps({"suggestions": []}, separators=(",",":"), indent=2), encoding="utf-8")
        info(f"[done] Wrote 0 suggestions to {OUT_JSON}")
        return

    total_candidates: List[Dict[str, Any]] = []
    total_files = 0
    for jf in java_files:
        total_files += 1
        src = jf.read_text(encoding="utf-8", errors="ignore")
        ast = run_ast_generator(jf)
        if not ast:
            warn(f"[scan] Skipping {jf.name} due to AST error.")
            continue
        file_candidates = build_candidates_from_ast(ast, src)
        info(f"[scan] {jf.name} → {len(file_candidates)} candidates")
        total_candidates.extend(file_candidates)

    if not total_candidates:
        warn("[scan] No candidates found in any file.")
        OUT_JSON.write_text(json.dumps({"suggestions": []}, separators=(",",":"), indent=2), encoding="utf-8")
        info(f"[done] Wrote 0 suggestions to {OUT_JSON}")
        return

    chunks = chunk_candidates(total_candidates, MAX_METHODS_PER_CHUNK)
    all_suggestions: List[Dict[str, Any]] = []

    for idx, ch in enumerate(chunks, 1):
        info(f"[llm] Processing chunk {idx}/{len(chunks)} with {len(ch)} candidates...")
        all_suggestions.extend(process_chunk_recursively(ch, DEFAULT_MODEL, DEFAULT_STRICT, idx))

    OUT_JSON.write_text(json.dumps({"suggestions": all_suggestions}, indent=2), encoding="utf-8")
    info(f"[done] Wrote {len(all_suggestions)} suggestions to {OUT_JSON}")
    info(f"[total] files≈{total_files} candidates≈{len(total_candidates)}")

if __name__ == "__main__":
    main()