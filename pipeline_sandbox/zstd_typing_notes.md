# zstd + Parquet typing — does it really shrink files that much?

Short answer: yes, but the headline numbers depend entirely on what's in your columns. This note explains *why* the reductions are real, where they come from, and how to read the output of `zstd_typing_demo.py`.

---

## 1. What zstd actually does

TODO xopen  and orsjson on all packages

zstd (Zstandard) is a general-purpose lossless compressor written by Facebook in 2016. It's the same family as gzip and snappy — they all find repeated byte sequences and replace them with shorter codes — but zstd has two practical advantages:

- **Better ratio than snappy at comparable speed.** Snappy is fast but barely compresses. zstd matches snappy's decompression speed and gets ratios closer to gzip.
- **Tunable.** Compression levels 1–22. Level 3 (the Polars default) is the universally-recommended sweet spot. Above 9 you pay a lot of write time for very little extra ratio.

zstd doesn't know anything about your data being a DataFrame. It's compressing raw bytes. So the question of *how much it shrinks your file* is really a question of *how repetitive the bytes are*.

That's why typing matters before compression.

## 2. Why typing makes compression dramatically better

Parquet is a **columnar** format. It stores all values of column A together, then all values of column B, then column C. Each column is compressed as its own block.

This matters because:

- A column of 200 000 strings that are mostly `"Fianna Fáil"` and `"Sinn Féin"` is *extremely* repetitive when stored together. zstd eats this for breakfast.
- A column of 200 000 random `Int64` payment amounts is *not* very repetitive. zstd can only do modest work here.

So compression ratio tracks column entropy, and the easiest way to lower entropy is to **declare the right dtype before writing**.

### Dictionary encoding (the `Categorical` trick)

If you cast `party` to `Categorical` before writing, Polars/Parquet stores:

- A small dictionary: `{0: "Fianna Fáil", 1: "Sinn Féin", 2: "Fine Gael", ...}` — about 100 bytes total.
- The column itself: 200 000 × 1 byte = 200 KB of integer codes.

Total ≈ 200 KB. **Before** zstd even runs.

Without categorical, the same column is 200 000 × ~12 bytes (UTF-8 of "Fianna Fáil") = 2.4 MB raw. zstd will compress it well because of the repetition, but it's working harder and reaching a worse final size than it would have with the integer codes.

The point: **dictionary encoding does most of the compression work for low-cardinality strings; zstd then compresses the leftovers.** They're complementary, not redundant.

### Integer downcasting

`Int64` → `Int32` halves the column footprint *before* compression. zstd will then compress the smaller column to roughly half the size of what it would have produced from the bigger column. So downcasting compounds with compression rather than competing with it.

For `year` columns (4-digit values), `Int32` is the right call. `Int16` would technically work (years fit in -32 768 to 32 767) but causes problems if anyone ever does arithmetic that overflows — `Int32` is the conventional choice.

## 3. Why the savings are real, not theoretical

The skepticism in the question — "this can't possibly be true" — is the right instinct, but the numbers do hold up because Parquet's typing model and zstd were designed to work together. The ratios are well-documented in the Apache Parquet and Polars test suites and reproducible in any benchmark.

What is *not* always true:

- **2× to 10× reductions** apply to files dominated by repeated low-cardinality strings. (Many of your gold tables fit this — `party`, `chamber`, `constituency`, `taa_band_label` repeat thousands of times.)
- **Modest reductions (10–30 %)** apply to files dominated by high-cardinality data: free-text narratives, unique IDs, payment amounts, dates. There's just less repetition for either dictionary encoding or zstd to exploit.

The demo script reports per-file ratios so you can see which of your files are which.

## 4. The compounding effect

When all three levers are applied (typing + dictionary + zstd), the savings multiply rather than add:

- Typing alone: maybe 30–50 % reduction (depending on how many `Int64` columns get downcast and how many strings are categorical-eligible).
- zstd alone on baseline: maybe 30–50 %.
- Both together: 60–85 %, because zstd is now compressing already-small columns.

This is why the doc claims "page loads visibly faster" — for the columns that matter most (filters), Polars/DuckDB read smaller blocks AND scan integer codes instead of string bytes.

## 5. What the demo measures

`zstd_typing_demo.py` reads every parquet in `data/gold/parquet/` and writes four versions of each into `pipeline_sandbox/_zstd_demo_out/`:

| Variant         | Compression | Typing changes                                       |
|-----------------|-------------|-------------------------------------------------------|
| `baseline`      | uncompressed| none — round-tripped as-is                            |
| `zstd_only`     | zstd-3      | none                                                  |
| `typed_only`    | uncompressed| Int32 years, Categorical low-cardinality cols         |
| `typed_zstd`    | zstd-3      | Int32 years, Categorical low-cardinality cols         |

It then prints a table with absolute sizes and percentage reductions, plus read-time deltas for each variant.

You can spot-check the demo by:
- Opening one of the produced files in DuckDB and running the same query against both versions — the result must be identical.
- Watching the per-column dtype dump that the script prints; you'll see exactly which columns got downcast or categorised.

## 6. Caveats and gotchas

- **Categorical join keys.** If you cast a column to `Categorical` and then join two DataFrames on it, the categorical mappings on the two sides may not match. The safe pattern is **cast to Categorical at write time only**, after all joins are done. The demo respects this — it casts immediately before the write.
- **DuckDB reads Categorical transparently.** No SQL changes needed; literal string filters still work (`WHERE party = 'Sinn Féin'`).
- **Compression level.** Level 3 is the right default. Higher levels write much slower for marginal gains. Don't tune unless you measure.
- **Row-group statistics.** Polars writes min/max-per-row-group statistics by default. With those, `WHERE year >= 2020` can skip whole row groups without decoding. This is independent of zstd but stacks with it.

## 7. Why this matters for Dáil Tracker specifically

Three of your gold parquets are obvious candidates for big wins:

- `current_dail_vote_history.parquet` (2.5 MB) — every row has `party`, `constituency`, `dail_term`, `chamber`. Predict 60–80 % reduction.
- `questions.parquet` silver (17 MB) — has many repeated member fields. Predict 50–70 %.
- `payments_fact.parquet` — has `taa_band_label` and member metadata repeating per payment. Predict 30–50 %.

Files dominated by free text (`narrative` in payments, debate titles) will reduce less.

Run the demo and see for yourself.

## 8. Empirical findings (2026-05-03)

After running the demo against actual gold and silver parquet files, the predictions in §7 turned out to be partially right and partially wrong. The real story is more nuanced and worth recording.

### 8.1 Compression behaviour depends on the *shape* of dominant columns

The headline finding: typing + zstd does not always compound the way the literature suggests. **Whether typing helps depends on what's in your widest columns.**

- **Wide tables of low-cardinality strings + numerics** (member-keyed rankings, attendance, lobbying joins): typing dominates. Categorical encoding shrinks the dictionary; zstd compresses the leftovers. Both levers compound. Expect 70–85 % reductions.
- **Tables dominated by long high-cardinality strings** (URIs, full text): zstd does almost all the work alone. Categorical helps with none of these columns because they have no repetition to dictionary-encode. Typing-on-top adds essentially nothing. Expect 75–90 % reductions from zstd alone.

For the second category, you'd think nothing else can be done. In fact something else can — see §8.2.

### 8.2 Questions silver — the URI-cleanup case study

`data/silver/parquet/questions.parquet` (120 139 rows, 26 cols) is a perfect example of a "long-string-dominated" table. Empirical numbers:

| Variant | Size | vs current on-disk |
|---------|------|---------------------|
| Current on-disk file (already zstd-compressed) | 16.95 MB | 0% |
| Drop 5 redundant URI cols + 4 Null cols, keep free text | **8.22 MB** | **−51.5 %** |
| Same plus typing on top | 8.22 MB | −51.5 % |
| `question_text` alone, zstd | 7.38 MB | −56.5 % |

The five URI columns dropped were all **100 % reconstructable from IDs already present in the table**:

| Dropped column | Reconstructable from |
|----------------|----------------------|
| `debate_section_uri` | `debate_section_id` + `context_date` |
| `uri` (the question's own API URI) | `debate_section_id` + `question_number` + `context_date` |
| `member_uri` | `unique_member_code` (literally same string with URL prefix) |
| `question.debateSection.formats.xml.uri` | same as `debate_section_uri` with `.xml` suffix |
| `question.house.uri` | `house` + `house_no` |

These five columns held ~28 MB of uncompressed bytes (compressed to ~9 MB) carrying zero information that wasn't already in the IDs. Once dropped:

- **`question_text` is ~89 % of the lean file's bytes.** Free text dominates.
- **Typing has zero effect.** The categorical-eligible columns are now too small a fraction of the file to register.
- **There is a "free-text floor" of ~7.4 MB** for 120k question texts. Below that you'd be sacrificing the prose itself.

### 8.3 The user-facing URL vs internal API URI distinction

Don't drop URLs reflexively. Some of them are user-facing and need to stay. The lean cleanup pattern needs to inspect each URI column individually:

| URI column | Which kind? | Action |
|------------|-------------|--------|
| `data.oireachtas.ie/akn/...` | Internal API (machine-readable XML/JSON) | drop, reconstruct from ID |
| `data.oireachtas.ie/ie/oireachtas/...` | Internal API entity reference | drop, reconstruct from ID |
| `www.oireachtas.ie/en/debates/...` | **Public web page a user clicks** | **keep** |

Debates silver is a clear example: it has both `uri` (api artefact, drop) and `debate_url_web` (public oireachtas.ie page, keep).

### 8.4 Helper: building user-facing URLs at render time

Once internal URIs are dropped from storage, build the URL when rendering:

```python
def question_url(context_date: str, question_number: int) -> str:
    return f"https://data.oireachtas.ie/ie/oireachtas/question/{context_date}/pq_{question_number}"

def member_url(unique_member_code: str) -> str:
    return f"https://data.oireachtas.ie/ie/oireachtas/member/id/{unique_member_code}"
```

This sits at the UI/render layer, not the pipeline. The pipeline keeps the IDs; the UI builds the URLs.

### 8.5 Debates silver — same pattern, smaller absolute saving

`data/silver/parquet/debates.parquet` (2 133 rows, 23 cols):

| Variant | Size | vs current on-disk |
|---------|------|---------------------|
| Current on-disk file | 189 KB | 0% |
| Lean (drop API URI + Null cols + single-value cols) | **120 KB** | **−36 %** |

Six columns dropped:
- `uri` (internal API URL — keep `debate_url_web` instead)
- `chamber.uri` (3 unique values, all internal)
- `billSort.billShortTitleEnSort`, `billSort.billYearSort` (Null)
- `bill.billType` (always `"Public"`)
- `bill.source` (always `"Private Member"`)

The percentage looks good but the absolute saving is 70 KB — irrelevant at this scale. The value here is **schema hygiene**, not size: dropping dead columns now means they don't carry forward as you ingest more years.

### 8.6 Forward projections — going back further

For questions, with the lean shape (free text preserved), per-row cost is ~70 bytes/row compressed. Linear projection:

| Years covered | Estimated rows | Estimated size |
|---------------|----------------|----------------|
| 7 (current) | 120 k | ~8 MB |
| 16 (back to 2010) | ~270 k | ~19 MB |
| 26 (back to 2000) | ~440 k | ~31 MB |
| 36 (back to 1990) | ~610 k | ~43 MB |

Real-world adjustment: pre-2010 Dálanna had far fewer written parliamentary questions per year (the 5 000-questions-per-week culture is recent). Older years probably contribute 30–60 % the volume of recent ones, so 36-year totals more realistically land at 25–35 MB.

Either way: **decades of full question text fit comfortably in ~30 MB.** Storage is not the bottleneck for going back further on these datasets — source ingestion and identity resolution are.

### 8.7 Updated cleanup pattern (apply at silver write)

This isn't a separate pass — it's a one-line change at each silver writer:

```python
# questions silver writer
DROP_COLUMNS = [
    "debate_section_uri", "uri", "member_uri",
    "question.debateSection.formats.xml.uri", "question.house.uri",
    "question.debateSection.formats.pdf",      # Null
    "question.to.uri",                         # Null
    "question.to.roleType",                    # Null
    "ministry_role_code",                      # Null
]
questions_silver = questions_raw.drop([c for c in DROP_COLUMNS if c in questions_raw.columns])
questions_silver.write_parquet(
    SILVER_DIR / "parquet" / "questions.parquet",
    compression="zstd", compression_level=3,
)
```

Done at the source of the write. No information is lost (every dropped column is reconstructable from kept columns or was already Null/single-valued). All free text preserved.

### 8.8 Lessons that change §7's predictions

- §7 predicted 50–70 % reduction on questions silver from typing + zstd. **Wrong on the mechanism** — typing did nothing because the dominant columns are high-cardinality. The real win came from dropping reconstructable URIs.
- §7 didn't mention URI cleanup at all. **It's the dominant lever for any table sourced from a REST API** that returns full URI references alongside IDs.
- The general principle: **before tuning compression, audit the schema.** Single-value columns, Null columns, and ID-vs-URL duplicates are free wins that compression can't substitute for.

## 9. Further reading

- [Apache Parquet — Encodings](https://parquet.apache.org/docs/file-format/data-pages/encodings/) — covers RLE, dictionary, and delta encodings.
- [Facebook zstd whitepaper](https://github.com/facebook/zstd) — compression algorithm design.
- [Polars `write_parquet` docs](https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_parquet.html) — exact arguments accepted.
- [DuckDB on Parquet](https://duckdb.org/docs/data/parquet/overview) — how DuckDB exploits row-group statistics and dictionary encoding for fast filters.
