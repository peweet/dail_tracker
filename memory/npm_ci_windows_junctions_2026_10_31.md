# npm installs and Windows dependency junctions

Observed on 2026-10-31 during an isolated Git integration checkout: `npm ci` followed a
`node_modules` junction and removed packages from its target. The installation was stopped;
the target's original lockfile was reinstalled, its original Next version was confirmed,
and its 19 focused web tests passed again. Source files were unchanged.

Before an install, require the destination's `node_modules` to be absent or an independently
owned real directory. Sharing dependencies through a junction is suitable only for operations
that will not install, prune or remove packages. Never run an install after a failed isolation
check, including when the shell would otherwise continue to the next command.

On Windows, Python 3.12 provides `Path.is_junction()`. After verifying both the intended link
path and its resolved target, `os.rmdir(link_path)` detaches that directory junction without
recursively deleting its target. An interpreter without the check must fail closed. Use a
separate dependency installation and the checkout's supported Node/npm versions for verification.

Evidence: session `01a19424-8800-7ee2-a6f2-ba598a95b1ec`; restored installation and focused
tests observed on 2026-10-31. This is one observed Windows/npm failure, not a claim about every
npm version or link implementation.
