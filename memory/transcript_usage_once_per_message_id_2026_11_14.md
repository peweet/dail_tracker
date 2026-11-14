# Transcript token usage is counted per API call, not per record

Claude Code writes one transcript record per content block of an assistant message: one for
the thinking block, one for the text, one for each tool call. Every one of those records
repeats the same `message.usage`. A script that adds usage per record therefore counts each
API call two to four times. In session `9b53970c` on 14 November 2026, 117 assistant records
held 46 API calls, and all 42 calls that were split across records repeated identical usage.

The repository's own tools made this mistake. `tools/token_week_review.py --days 1` reported
2.98 billion cache-read tokens and 7,641 turns. Counted once per `message.id` it reports 1.26
billion and 3,190 turns. The share of cache-read billed above 400k context barely moved (66%),
so ratios quoted from earlier reviews still hold, but their totals and turn counts were
inflated. The status line's estimated cost was inflated the same way.

On 14 November `token_week_review.py`, `token_trend.py` and `statusline_cost.py` were changed
to count usage once per message id while still counting content blocks per record. Two were
left alone on purpose. `tools/token_ledger.py` and the `tools/hooks/session_token_ledger.py`
hook still count records, and their counts feed the closeout gate's `TURNS_MIN = 500`, which
was calibrated on record counts. Changing them moves that gate, so it waits for the owner.

Thinking text is not kept in the transcripts either. On 13 November the main sessions produced
4.67 million output tokens, of which about 1.18 million are visible text and tool input. The
rest is thinking. A context model built only from transcript text under-counts what each call
carries.

Any new script over transcripts should deduplicate usage by `message.id`.
