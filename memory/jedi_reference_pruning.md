# Prune reference-search directories before traversal

Filtering Jedi's yielded Python files does not bound filesystem traversal. Its
reference search calls `FolderIO.walk()` first, so ignored caches, private trees
and sandboxes can consume most of a verification run even when no excluded file
is parsed.

Build the permitted directory ancestors from the existing Git-visible scan
allowlist and prune the mutable folder list before descending. Preserve that
same list for Jedi's own exclusions, keep the final file filter, and retain the
existing reference-search lock and restoration of Jedi globals.

Prove the boundary through a real cross-file `refs()` query in a small temporary
Git repository: reject `os.scandir` inside an ignored subtree and still require
the public caller in the returned references. Output filtering alone cannot pass
that contract. Also retain the real-repository total/count and response-cap checks.

Evidence: `mcp_server/py_refs.py::_filtered_jedi_recurse` and
`test/mcp_server/test_py_refs.py::test_refs_prunes_git_ignored_subtree_before_descent`.
The isolated test failed with "policy-excluded subtree was traversed" before
the repair and passed afterward. A sampled stalled verification stack was inside
Jedi's directory recursion; process existence alone was not treated as progress.

Directory pruning does not govern imports. Fresh-Script module-resolution guards
keep excluded project modules unresolved; a separate parser guard covers direct
type-stub loads. Preserve and restore the instance overrides, and permit Jedi
runtime inference stubs. The resolver subprocess can already have read bytes: this
is parse containment, not filesystem isolation. Compare Git-relative path keys as
native `Path` objects so Windows case equivalence does not hide allowed sources.

The independently raised Python-import, type-stub and Windows-case cases each
failed before repair and their actual test run passed afterward. Retain recorder
interruptions separately from completed native receipts.
