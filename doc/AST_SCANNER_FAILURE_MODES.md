---
tier: 1
status: LIVE
domain: engineering
updated: 2026-08-04
read_when: auditing an AST-based code scanner, contract extractor, repository indexer, or migration ratchet for false positives and false negatives
key: AST scanner failure modes docstrings discovery aliases sessions containment limits paths fail-closed regression tests
---

# AST scanner failure modes — audit and regression guide

## Executive summary

The Python AST was not the problem. The failures came from treating “we call
`ast.parse()`” as proof that the whole scanner was structural and complete.

A trustworthy scanner is a pipeline:

```text
scope policy → file discovery → decoding → parsing → binding analysis
             → structural extraction → normalization → comparison → CI gate
```

The scanner is only as sound as its weakest stage. In this project, some tools
parsed Python correctly but still:

- counted examples inside module, class, and function docstrings as live code;
- scanned only two convenient directories rather than the complete runtime;
- recognized `requests.get(...)` but missed aliases, imported functions, and
  `Session` calls;
- swallowed parse failures, allowing unreadable files to disappear from results;
- applied privacy/exclusion policy during automatic discovery but not when a
  caller supplied an explicit file or directory;
- limited top-level definitions while allowing one large class to return every
  nested method;
- compared only one fragment of a generated contract, so route changes could
  pass while parameter names stayed the same;
- resolved CLI outputs relative to the caller's current working directory;
- generated documentation that named an old, nonexistent script path; and
- had synthetic unit tests but no CI command that ran the scanners against the
  real repository.

Those bugs produce both kinds of dangerous result:

- **False positive:** documentation or excluded code is reported as live debt.
- **False negative:** a live bypass or contract change is reported as clean.

This guide records the failure modes, robust implementation patterns, and a
copyable test matrix for checking whether the same defects exist elsewhere.

## What was wrong here

| Area | Faulty assumption | Failure produced | Corrective pattern |
|---|---|---|---|
| Docstrings | Every string node in `ast.walk()` is live code | Examples such as `requests.get(...)`, `class=...`, or `?member=` created false findings | Identify structural docstring statements and do not traverse them |
| Discovery | Scanning `extractors/` and `iris/` represented the runtime | Refresh chains, planning extractors, PDF pollers, Wikidata jobs, and tools were invisible | Define runtime roots explicitly, recurse, and test representative files from every root |
| HTTP calls | Direct calls always look like `requests.get(...)` | `import requests as rq`, `from requests import get`, and `session.get` escaped | Build an import-binding prepass and track session-producing assignments/context managers |
| Parse errors | A broken file can be skipped | Syntax/encoding failures made the report look cleaner | Fail closed: collect errors, print paths and causes, return nonzero |
| Encoding | Every Python file is UTF-8 | PEP 263 encoded source could fail or be misread | Use `tokenize.open()` before `ast.parse()` |
| Definitions | Top-level regex/AST names are an adequate class contract | Bases, decorators, keywords, signatures, async defs, and nested methods were incomplete | Extract real `ClassDef`/`FunctionDef`/`AsyncFunctionDef` fields recursively |
| Output limits | Limiting the top-level list limits the response | One large class bypassed the budget through hundreds of nested methods | Count every returned definition recursively and clamp the client limit to a server maximum |
| Scan policy | Filtering discovered paths is sufficient | Explicit paths could open dot, private, sandbox, or generated trees | Apply policy to lexical and resolved paths on every entry path, including recursion |
| Containment | String prefix checks keep paths inside the repository | `..`, absolute paths, drive-qualified paths, or symlinks could escape/bypass policy | Reject absolute/drive paths, resolve, and require `relative_to(root)` |
| Contract drift | Equal query-parameter sets mean equal URL contracts | A route path, title, module, duplicate, or registration could change undetected | Compare normalized route records and parameter records; ignore only deliberate volatile fields |
| Paths | Relative paths are harmless in developer tools | Output/read locations changed with process CWD and wheel installation | Separate code-resource root from configurable data/runtime roots; resolve CLI paths explicitly |
| Delivery | Unit tests prove the live scanner is enforced | The real repository could drift while CI remained green | Put the real `--check` commands in the canonical local and CI profiles |

## Failure mode 1: docstrings are AST nodes too

### Symptom

A scanner reports a call, URL parameter, CSS class, HTML fragment, or import that
appears only in documentation.

This happens because a docstring is represented as an `Expr` whose value is a
string `Constant`. A plain `ast.walk(tree)` visits that constant exactly like a
live string assigned to a variable.

```python
"""Example only: requests.get("https://example.invalid")"""

class Client:
    """Example markup: <div class="not-live"></div>"""

    def fetch(self):
        """Example link: /?not_a_real_parameter=1"""
        return shared_fetch("https://example.test")
```

A text grep finds all three examples. A naive AST string scan also finds them.
Neither result describes executable behavior.

### Robust pattern

Identify docstring *statements* structurally and prune their whole subtrees. Do
this for modules, classes, synchronous functions, and asynchronous functions.

```python
from __future__ import annotations

import ast
from collections.abc import Iterator


_DOC_OWNERS = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def docstring_statements(tree: ast.AST) -> set[ast.Expr]:
    blocked: set[ast.Expr] = set()
    for owner in ast.walk(tree):
        if not isinstance(owner, _DOC_OWNERS) or not owner.body:
            continue
        first = owner.body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            blocked.add(first)
    return blocked


def live_walk(tree: ast.AST) -> Iterator[ast.AST]:
    blocked = docstring_statements(tree)
    stack = [tree]
    while stack:
        node = stack.pop()
        if node in blocked:
            continue
        yield node
        stack.extend(reversed(list(ast.iter_child_nodes(node))))
```

Do not remove every string literal. Live strings are often exactly what a URL,
markup, SQL, or policy scanner needs to inspect. Remove only strings occupying a
structural docstring position.

### Required regression probes

- Module docstring example is ignored.
- Class docstring example is ignored.
- Function and async-function docstring examples are ignored.
- A second string expression after the docstring is still scanned.
- An assigned string, f-string, concatenation, and keyword argument remain live.

## Failure mode 2: parsing was structural, discovery was not complete

### Symptom

The scanner correctly analyzes every file it sees, but its “zero findings” claim
does not cover the full runtime.

The cloud/network audit originally recursed through only `extractors/` and
`iris/`. Live network work also existed in refresh entry points, planning civic
extractors, lobbying, members, PDF infrastructure, reference jobs, services,
Wikidata, and selected pipeline tools. A precise scanner over an incomplete file
set is still a false-green scanner.

### Robust pattern

Treat discovery as a versioned contract, not an incidental glob.

```python
RUNTIME_ROOTS = (
    ROOT / "api",
    ROOT / "extractors",
    ROOT / "lobbying",
    ROOT / "members",
    ROOT / "pdf_infra",
    ROOT / "planning" / "civic" / "extractors",
    ROOT / "reference",
    ROOT / "services",
    ROOT / "wikidata",
)

ROOT_ENTRYPOINTS = tuple(ROOT.glob("*_refresh.py")) + (ROOT / "pipeline.py",)


def discover_python() -> list[Path]:
    files = {path for root in RUNTIME_ROOTS for path in root.rglob("*.py")}
    files.update(path for path in ROOT_ENTRYPOINTS if path.is_file())
    return sorted(path for path in files if SCAN_POLICY.allows(path.relative_to(ROOT)))
```

The exact roots will differ at work. The important properties are:

- the list is explicit and reviewable;
- recursion is used for package trees;
- root entry points and scheduled jobs are included;
- sandbox/test/generated exclusions are centralized in policy;
- a test pins at least one known file from every runtime area; and
- the report prints its scope and file count.

Compare the discovered set with packaging metadata, CLI entry points, service
manifests, schedulers, Docker `COPY` paths, and pipeline registries. Directory
names alone do not define what production runs.

## Failure mode 3: call matching understood spelling, not bindings

### Symptom

The scanner finds one syntactic spelling but misses equivalent calls:

```python
import requests
requests.get(url)

import requests as rq
rq.get(url)

from requests import get as download
download(url)

client = requests.Session()
client.get(url)

with requests.Session() as session:
    session.post(url, data=payload)
```

A check for only this shape is insufficient:

```python
isinstance(call.func, ast.Attribute)
and isinstance(call.func.value, ast.Name)
and call.func.value.id == "requests"
and call.func.attr in {"get", "post", "head"}
```

### Robust pattern

Use a two-stage analysis:

1. Build bindings from `Import` and `ImportFrom` statements.
2. Track simple values created from a known `Session` factory through `Assign`,
   `AnnAssign`, and `with ... as ...`.

At minimum, retain:

- module aliases: `rq -> requests`;
- imported function aliases: `download -> requests.get`;
- session factories: `requests.Session`, aliased equivalents;
- session variables: `client`, `session`;
- approved shared-helper aliases; and
- source location and resolved call name for the report.

An expression-name helper keeps attribute handling consistent:

```python
def dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None
```

This is intentionally a bounded static analysis, not a complete Python data-flow
engine. Document its limits. If a session is passed through several functions,
stored on `self`, returned by a factory, or selected dynamically, either expand
the analysis conservatively or require an explicit reviewed annotation.

Apply the same reasoning to every client library in scope, including `httpx`,
`aiohttp`, and `urllib`, and to async context managers. Do not claim “all HTTP”
when the binding table recognizes only one library.

### Stateful HTTP is not automatically a violation

Cookie, VIEWSTATE, WAF-clearance, multi-hop form, streamed download, and circuit-
breaker workflows legitimately need a session. The policy should distinguish:

- stateless calls that must use the common fetch helper;
- stateful calls that must use a common retry-configured session factory; and
- exceptional transports whose exact streaming/HEAD/circuit behavior is reviewed
  and listed with a per-file rationale.

An unexplained allow-list is a blind spot. A report section titled “reviewed
transports,” with the file and reason, is an auditable exception.

## Failure mode 4: parse and decode errors failed open

### Symptom

A malformed, unsupported, or differently encoded source file disappears from the
results, often because the scanner catches `Exception` and continues.

That makes an unreadable file indistinguishable from a clean file.

### Robust pattern

Use Python's encoding-cookie-aware reader and make errors part of the result.

```python
import ast
import tokenize
from pathlib import Path


class AnalysisError(RuntimeError):
    def __init__(self, path: Path, cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"{path}: {type(cause).__name__}: {cause}")


def parse_module(path: Path) -> ast.Module:
    try:
        with tokenize.open(path) as stream:
            source = stream.read()
        return ast.parse(source, filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise AnalysisError(path, exc) from exc
```

For a repository-wide scan, it is acceptable to collect multiple errors before
returning. It is not acceptable to exit zero while errors exist. Print the
relative file, exception type, and useful location without exposing private
absolute paths in a public report.

Test at least:

- invalid syntax;
- an unreadable/missing file;
- a valid PEP 263 non-UTF-8 file; and
- a syntax feature newer than the scanner interpreter, if mixed Python versions
  are supported.

## Failure mode 5: class and function contracts were incomplete

### Symptom

A “class scanner” is based on regex, top-level nodes only, or names without the
structural information downstream tooling needs.

A proper definition record should be derived from actual nodes:

- `ClassDef`;
- `FunctionDef`;
- `AsyncFunctionDef`.

For classes, preserve name, qualified name, line span, decorators, bases,
keywords such as `metaclass=`, type parameters where supported, one-line
docstring, nested classes, and methods. For functions, preserve async/sync kind,
signature, decorators, return annotation, line span, one-line docstring, and
nested definitions where the use case requires them.

Use `ast.unparse()` for display where available. Never import or execute the
scanned module just to inspect it: imports can perform I/O, read secrets, or
depend on unavailable native libraries.

### Why top-level-only limits are unsafe

This result appears bounded but is not:

```python
top_level = definitions[:limit]
```

If the first entry is a class containing 800 methods and serialization includes
all methods, `limit=1` can still return 801 definition records.

Budget recursively. Count the parent shell, then descendants, and stop when the
remaining budget reaches zero. Preserve an included parent shell so the caller
understands where returned children belong. Separately clamp caller input:

```python
MAX_OUTLINE_DEFINITIONS = 200
cap = max(1, min(requested_limit, MAX_OUTLINE_DEFINITIONS))
```

Return `truncated`, `returned_definition_count`, and the hard maximum. Test one
large class, nested classes, nested functions, `limit=1`, a negative limit, and
an extremely large client value.

## Failure mode 6: policy applied to discovery but not explicit targets

### Symptom

Automatic indexing excludes private or generated paths, while an explicit API
such as `outline(".agents/...")` or `outline("pipeline_sandbox/...")` opens them.

This occurs when policy is used only inside the discovery loop. Every entry path
must enforce the same policy.

### Robust containment and policy sequence

1. Parse the request as a path.
2. Reject absolute and drive-qualified paths.
3. Normalize separators for the lexical policy check.
4. Reject dot/private/sandbox/generated patterns lexically.
5. Resolve against the allowed root.
6. Require the resolved target to be contained by that root.
7. Apply policy again to the resolved repository-relative path.
8. For directories, apply policy to every recursive child and listed subpackage.

```python
requested = Path(user_value)
if requested.is_absolute() or requested.drive:
    raise ValueError("path must be repository-relative")

lexical = PurePosixPath(user_value.replace("\\", "/"))
if not policy.allows(lexical):
    raise PermissionError("path is outside scan policy")

target = (root / requested).resolve()
relative = target.relative_to(root.resolve())  # raises on traversal/symlink escape
if not policy.allows(relative):
    raise PermissionError("resolved path is outside scan policy")
```

Do not use `str(target).startswith(str(root))`; sibling names such as
`/work/repo-private` can share that prefix. Use `Path.relative_to()` or
`Path.is_relative_to()` on resolved paths.

Pin direct requests for:

- `.hidden/file.py`;
- `private_notes/file.py`;
- `pipeline_sandbox/file.py`;
- generated/cache/vendor paths;
- `../outside.py`;
- an absolute path;
- a drive-qualified Windows path; and
- a symlink that resolves outside the root, where the test platform permits it.

## Failure mode 7: generated-contract checks compared too little

### Symptom

The URL contract checker compared only the set of query-parameter names. These
changes could therefore pass:

- `url_path` changed;
- route title changed;
- page module changed;
- route removed or duplicated; or
- a route was re-registered under a different callable.

### Robust pattern

Compare normalized semantic records, not one extracted subset and not volatile
rendering details.

For a route table, a useful record is:

```python
(url_path, title, module)
```

Keep duplicates by comparing sorted lists or counters, not sets. Compare query
parameter records separately. Ignore `app.py` line numbers because moving code
without changing behavior should not invalidate a public URL contract.

The principle generalizes:

- API contracts: compare method, path, success schema, and error schemas.
- CSS contracts: compare emitted class vocabulary and defined selectors.
- CLI contracts: compare command names, options, defaults, and exit behavior.
- Data contracts: compare names, types, nullability, grain, and semantic enums.

For each contract, write a mutation test: change exactly one load-bearing field
while holding the others constant and prove `--check` fails.

## Failure mode 8: relative filesystem behavior made tools environment-dependent

### Symptom

The same scanner writes or reads a different file when launched from the IDE,
repository root, a subdirectory, CI, a wheel, or a service manager.

`Path("doc/report.md")` is relative to the process CWD, not to the script or
project. Calling `.resolve()` does not fix the ambiguity; it merely makes the
current ambiguity absolute.

### Robust pattern

Separate three concepts:

- **Resource root:** packaged code, SQL, schemas, templates.
- **Data root:** deployer-provided or pipeline-produced datasets.
- **Runtime root:** transient caches, downloads, logs, locks, and export files.

```python
RESOURCE_ROOT = Path(__file__).resolve().parents[1]


def absolute_path(value: str | Path, *, base: Path = RESOURCE_ROOT) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    return candidate.resolve()


DATA_ROOT = absolute_path(os.environ.get("APP_DATA_DIR", "data"))
```

For a CLI output, choose and document one policy:

- require an absolute path; or
- anchor relative values to a declared project/resource root.

Do not silently use CWD. Create parent directories only in the explicit write
operation, not at module import time. When replacing a committed generated
artifact, write a sibling temporary file and atomically replace the destination
so an interrupted scan cannot leave a partial baseline.

An installed wheel deserves its own smoke test. Importing the package is not
enough: start the service outside the checkout with an external data fixture and
probe a data-backed readiness condition.

## Failure mode 9: documentation and CI were outside the contract

### Stale generated commands

After scanners moved under `tools/migration/`, generated headers still instructed
users to run `tools/extract_url_contract.py`. The check ignored prose, so it could
pass while the regeneration command was invalid.

Keep the invocation path in one constant or test generated instructions. After a
move, search docs, baselines, workflow files, and help text for the old path.

### Synthetic tests without a live gate

Unit tests proved scanner functions on temporary files, but CI did not invoke the
real URL, class, and markup checks over the repository. Both layers are needed:

- synthetic tests isolate edge cases and mutations;
- the live `--check` command proves current code matches the committed baseline;
- the canonical local `check` profile and CI run the same command.

Record scope counts in output. An unexpected drop from 900 scanned modules to 90
should be visible even if the finding count remains zero.

## Reference scanner architecture

Keep stages separate enough to test independently:

```text
ScanPolicy
  ↓
discover(root, policy) -> list[relative Path]
  ↓
read_source(path) -> decoded str | AnalysisError
  ↓
parse_source(path, text) -> ast.Module | AnalysisError
  ↓
collect_bindings(tree) -> imports, aliases, sessions, helpers
  ↓
analyze(tree, bindings) -> typed findings/definition records
  ↓
normalize(records) -> stable semantic contract
  ↓
compare(current, baseline) -> additions/removals/errors
  ↓
render(report) + nonzero exit on drift or analysis error
```

Recommended invariants:

- Policy is pure and takes a repository-relative `Path`.
- Discovery returns deterministic, sorted, de-duplicated paths.
- Reading honors Python encoding declarations.
- Parsing never imports the target.
- Analysis excludes structural docstrings but retains live literals.
- Findings carry relative path, line, column, rule, and resolved call/symbol.
- Normalization deliberately excludes only documented volatile fields.
- Limits are enforced during recursive construction and again at serialization.
- Every error is visible and makes a gate fail closed.

## Copyable regression-test matrix

Use synthetic files because every row should mutate one property at a time.

| Test | Expected outcome |
|---|---|
| Module/class/function docstrings contain a forbidden call | No finding |
| Same call appears in a function body | Finding |
| Nested package contains a live call | File is discovered and finding appears |
| `import requests as rq; rq.get(...)` | Finding |
| `from requests import get as fetch; fetch(...)` | Finding |
| `client = requests.Session(); client.get(...)` | Finding |
| `with requests.Session() as client: client.post(...)` | Finding |
| Approved shared helper is imported with an alias | Classified as centralized, not a bypass |
| Source has invalid syntax | Scanner returns nonzero and names the file |
| Source has a valid PEP 263 encoding cookie | Parses successfully |
| Explicit dot/private/sandbox/generated file | Denied |
| Explicit allowed directory with a denied child | Child and denied subpackage omitted |
| `../`, absolute, drive-qualified, or escaping symlink path | Denied |
| One class contains more nested methods than the limit | Recursive returned count stays within cap |
| Client requests an enormous limit | Server hard maximum wins |
| Route title/path/module changes but parameters do not | Contract check fails |
| Only source line numbers move | Contract check passes |
| Duplicate route is added | Contract check fails |
| CLI is run from a temporary CWD | Reads/writes the declared absolute location |
| One discovered root is removed | Scope/completeness test fails |
| Live repository baseline drifts | CI `--check` command fails |

One particularly valuable meta-test compares discovery with an independently
maintained runtime inventory. Do not derive both sides from the same constant, or
the test will reproduce the bug it is meant to detect.

## How to audit another project

### 1. Inventory every scanner and every way it runs

Search for:

```text
ast.parse
ast.walk
NodeVisitor
tokenize.open
rglob("*.py")
glob("*.py")
read_text
errors="ignore"
except SyntaxError
except Exception
--check
baseline
```

For each tool, record its input roots, exclusions, decoder, parse-error behavior,
normalization, output path policy, baseline, CLI command, and CI caller.

### 2. Prove the scope independently

Compare scanned files with:

- package/build manifests;
- service and CLI entry points;
- schedulers/workflows;
- Docker `COPY` paths;
- plugin registries;
- pipeline task registries; and
- a repository file listing such as `rg --files -g '*.py'`.

Classify omitted files as intentional or accidental. Put intentional exclusions
in one policy object with rationale.

### 3. Seed adversarial syntax

Create a temporary mini-repository containing every alias/session/docstring case
from the matrix. Do not rely on examples already present in production: absence of
a syntax form today does not prove the scanner will catch it tomorrow.

### 4. Test failure behavior

Break one file deliberately. If the tool prints a warning and exits zero, it is
not a safe gate. Repeat with an encoding-cookie file and a permission/read error.

### 5. Mutate the contract

Change one route title, class base, decorator, parameter owner, or nested method.
Hold unrelated fields constant. Confirm the live check fails, regenerate, and
confirm it passes.

### 6. Run away from the repository root

Run help, check, and output commands from a temporary directory. Verify that all
reported paths are stable and all outputs land at documented absolute locations.

### 7. Verify CI executes reality

Find the exact workflow line that invokes the real scanner. A unit test importing
its helper is not equivalent. Confirm the CI dependency profile can import every
capability, and that a scanner error cannot be converted into an allowed warning.

## Review checklist

- [ ] Scanner scope matches the complete runtime inventory.
- [ ] Recursive package discovery is used where packages can nest.
- [ ] One centralized policy defines private, dot, sandbox, generated, cache, and vendor exclusions.
- [ ] The policy applies to automatic and explicit targets, before and after path resolution.
- [ ] Absolute, drive-qualified, traversal, and symlink-escape paths are rejected.
- [ ] Python files are decoded with `tokenize.open()`.
- [ ] Syntax, decoding, and I/O errors fail the gate closed.
- [ ] Module, class, sync-function, and async-function docstrings are pruned structurally.
- [ ] Live constants, f-strings, concatenations, positional args, and keyword args remain visible.
- [ ] Import aliases and `from ... import ... as ...` bindings are resolved.
- [ ] Session variables from assignments, annotations, and context managers are tracked.
- [ ] Stateful network exceptions use a shared retry session or carry a reviewed rationale.
- [ ] Class/function records come from actual AST definitions and include nested structure.
- [ ] Recursive output counts obey a server-side hard maximum.
- [ ] Contract comparison includes every semantic field and preserves duplicates.
- [ ] Volatile fields such as source line numbers are deliberately normalized out.
- [ ] Code-resource, external-data, and transient-runtime roots are separate.
- [ ] Relative CLI paths have an explicit anchor or are rejected.
- [ ] Generated docs contain valid current commands.
- [ ] Synthetic mutation tests and the live repository `--check` both run in CI.
- [ ] Reports state files scanned, files excluded, reviewed exceptions, parse errors, and truncation.

## When AST is not enough

Python AST is appropriate when the question is structural and Python-specific:
imports, calls, definitions, decorators, literal arguments, and static contracts.
It deliberately loses comments and formatting and cannot fully resolve dynamic
behavior.

Use a concrete syntax tree when comments/formatting are part of the contract. Use
Tree-sitter or language-native parsers for a multi-language repository. Use
runtime instrumentation only when static analysis cannot answer the question,
and keep it isolated because importing arbitrary modules can execute side effects.

No parser makes discovery, policy, containment, comparison, or CI enforcement
automatic. Those remain explicit engineering responsibilities.

## Acceptance criteria for a trustworthy scanner

A scanner is ready to act as a gate when all of the following are true:

1. Its claimed scope is explicit, independently tested, and printed.
2. Equivalent syntax spellings are either recognized or documented as limits.
3. Documentation examples cannot become live findings.
4. Unreadable source cannot become a clean result.
5. Explicit input cannot bypass discovery policy or repository containment.
6. Recursive outputs cannot bypass response budgets.
7. Baseline comparison covers the entire semantic contract.
8. Filesystem behavior is independent of process CWD and installation layout.
9. A real repository check—not only unit tests—runs in the standard local and CI gates.
10. A reviewer can explain every exclusion and exception from the generated report.

If any one of these is missing, describe the tool as a best-effort survey rather
than a correctness or security gate.
