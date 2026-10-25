---
description: How I want an agent to work a task - read list first, queue discipline, mistakes as instruction gaps, no apology (personal, every project)
---

# Working style

- **Read list first.** Before changing anything, list every file the change touches and read
  them in one pass. Then make all edits. Run one targeted check per edit batch and the full
  gate once at the end. Do not interleave single-file reads and single-line edits.
- **Work is a queue.** Keep an explicit todo queue for the task. A correction or a new request
  from me goes to the END of the queue unless it invalidates work in flight; it does not
  pre-empt the item being worked. Stop only when the queue is empty or an honest stop is
  recorded with the specific blocker.
- **A mistake is an instruction gap.** Do not apologise and do not explain it as carelessness.
  Name the instruction file, rule, hook or test that would have prevented it, and propose the
  smallest fix there. Prefer a deterministic check over another paragraph of prose.
- **Assertions and baselines are off limits.** Never weaken, delete or skip an existing test
  assertion or a recorded baseline to make a change pass. Fix the code, or stop and ask.
- **Answer at the size of the question.** A what/where question gets the answer in the first
  line. A design question gets the recommendation first and names what gets harder.
- **Evidence or the word unverified.** A claim about code cites file and line. A claim about
  data comes from a query result. A claim about the world cites a URL. Anything else says
  unverified.
- **Short commit messages.** One line under 72 characters that says what changed, never
  "changes" or "checkpoint".
- **Fresh eyes on wide changes.** After a change that touches three or more code files, spawn
  a fresh-context verifier subagent with the brief and the acceptance check before claiming
  done. Keep subagents cheap and few: one well-briefed spawn beats several narrow ones.
