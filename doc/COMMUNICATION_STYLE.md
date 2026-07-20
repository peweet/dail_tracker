# Communication style — full reference

**Status:** ACTIVE · **Domain:** process · **Loaded:** on demand only.

The always-loaded rule is [.claude/rules/communication.md](../.claude/rules/communication.md) — the
register table and the hard bans. This file is the corpus behind it: the full rules, the sources,
and the worked examples. Read it when auditing style, extending the rule file, or building the
linter. It is not loaded into every session.

Sources are quoted, not paraphrased. Where two guides disagree, the disagreement is recorded
rather than resolved silently.

---

## 1. Why this exists

The stated defect is jargon and wordiness. Both have three distinct causes, and they need
different fixes:

1. **Wrong register** — answering a one-line question at design-doc length. Fixed by §2.
2. **Borrowed abstraction** — reaching for an impressive word instead of the plain one. Fixed
   by §4, which is the largest section because this is the dominant failure.
3. **Padding** — filler that survives because it sounds considered. Fixed by §7.

---

## 2. Register gradation

Register is chosen by **what the question asks for**, not by how much I know. The dominant
error is escalation: an R0 question answered at R2 length.

### R0 — Lookup
Trigger: "what is", "where is", "does X", a name, a number, a path, a yes/no.
Shape: the answer in the first word. No heading, no lead-in, no recap, no offer of next steps.
Ceiling: 1–3 lines. A one-line answer is a complete answer.

### R1 — Explain
Trigger: "why does", "how does", "what's the difference".
Shape: answer in sentence one; mechanism after. Prose, not bullets.
Ceiling: 1–3 short paragraphs.

### R2 — Build (dev features) — see §3
Trigger: "add", "change", "design", "should we", "is it worth".
Shape: recommendation, then reasoning, then trade-offs.
Ceiling: scaled to the change — see the Ubl sizing rule in §3.

### R3 — Findings — see §5
Trigger: "what does the data show", "is this real", "how many".
Shape: figure, then what it can and cannot support.
Ceiling: ~300 words of prose (ONS overview budget).

**Escalation rule.** Escalate only when the answer would be *wrong* without the extra
structure, and say why in one clause. Never escalate silently. De-escalate freely: an R2
question with an obvious one-line answer gets the one line.

---

## 3. R2 — Dev features and design decisions

Weighted heaviest of the four registers. Source: Malte Ubl, *Design Docs at Google*
(https://www.industrialempathy.com/posts/design-docs-at-google/) and *Design docs — a design doc*
(https://www.industrialempathy.com/posts/design-doc-a-design-doc/), plus 37signals' decision
questions (https://37signals.com/how-we-make-decisions) and Amazon's narrative memo.

### The core claim
The document exists to record **trade-offs**, not to describe the code — the code already
describes the code. Ubl: the design doc is *"the place to write down the trade-offs you made in
designing your software."*

### Rules

**Lead with the recommendation.** Not a survey that ends in a choice. Ubl: the design section
*"begins with overview, progresses to details"* — never make the reader assemble the shape from
the parts.

**Non-goals are things that could reasonably have been goals** and were deliberately excluded.
Ubl, verbatim: *"non-goals aren't negated goals like 'The system shouldn't crash', but rather
things that could reasonably be goals, but are explicitly chosen not to be goals."*
- Real non-goal: "not backfilling pre-2019 payments in this pass"
- Noise: "not going to break the firewall"

**Alternatives considered must name the trade-off each option makes.** Ubl: *"The focus should
be on the trade-offs that each respective design makes and how those trade-offs led to the
decision."* An alternatives list with no trade-offs is decoration.

**State what gets harder.** 37signals: *"What gets easier if we make this decision? What gets
harder? Will easier remain easier in the long term, or is it short-term easy but long-term
hard? And vice versa."*

**Classify by reversibility and calibrate depth to it.** 37signals: *"How easily can we reverse
the decision?"* A one-way door earns paragraphs; a two-way door earns a sentence.

**Name the missing information that would change the answer.** 37signals: *"What missing
information would lead to making a different decision?"* This replaces diffuse hedging with a
specific, resolvable gap — and it is the §7 hedge ban's positive form.

**Include the do-nothing option.** 37signals: *"What would happen if we just didn't make the
decision?"*

**Steelman the other side.** 37signals: *"Why would someone else make a different decision?
What's the other side — or two or three — look like?"*

**Flag which principles the decision bends** rather than pretending it's consistent with all of
them. 37signals: *"What principles are we bending if we make this decision?"* In this repo that
means naming it when a change strains the logic firewall, the never-union grain rules, or the
provenance boundary.

**Check for retrofitted justification.** 37signals: *"What was our first instinct on this
decision? Are we now just walking around in circles trying to justify that gut reaction with
data?"*

**Say how we'll know it was right.** 37signals: *"When and how will we know whether the decision
was the right one, or if it even mattered?"*

**Size to the change.** Ubl: 10–20 pages for a large project, 1–3 for an incremental one.
Scaled to chat: a new subsystem gets structure; a flag rename gets a sentence.

**Explain why before asking for an action.** Google TW2: *"Before you ask your reader to perform
a task, explain to them why they are doing it."*

**Cross-cutting concerns get a standing mention** where they apply — Ubl lists security,
privacy, observability. In this repo the recurring set is: privacy quarantines, the three money
grains, the join-key semantics (`0 = not-matched ≠ absent`), and row-floor guards.

### Design-doc furniture that does NOT transfer to chat
Importing these produces bureaucratic padding: scope/non-scope preamble, stated audience
("this is aimed at engineers"), table of contents, author-and-date header, and Amazon's Purpose
section. The audience is in the room and the date is now.

### From Amazon's narrative memo — what does transfer
Source: https://www.aboutamazon.com/news/company-news/2017-letter-to-shareholders and
https://aws.amazon.com/blogs/startups/startup-advice-how-to-write-a-narrative/

**Prose over bullets, because bullets hide the links between claims.** Bezos: *"We don't do
PowerPoint (or any other slide-oriented) presentations at Amazon. Instead, we write narratively
structured six-page memos."* The mechanism matters more than the rule: a bulleted argument lets
an unsupported leap sit in whitespace where prose would have forced a connective. This is the
single most relevant Amazon lesson for chat replies, and the reason §6 makes prose the default.

**Ban speculative modals; cite instead.** AWS: *"Everything in your narrative should be based on
facts rather than speculation. Don't use words like 'should.' Instead, back everything up with
research, including internal and external data."* This is the same rule as `evidence.md`'s
hedging ban, arrived at independently.

**Write for a reader with no prior context.** AWS: the narrative *"should be written so that
anybody within your organization can read and understand it without any prior knowledge of the
proposal."* Chat caveat: the user has full repo context. Apply this to *reasoning* (don't skip
the step that makes the conclusion follow), not to *background* (don't re-explain the firewall).

**A good document takes longer than you think.** Bezos: *"great memos are written and re-written,
shared with colleagues who are asked to improve the work, set aside for a couple of days, and
then edited again with a fresh mind. They simply can't be done in a day or two."* The chat
analogue is §7 — one editing pass, always.

### Tenets — ordered preferences, not virtues
Source: https://docs.aws.amazon.com/prescriptive-guidance/latest/strategy-cloud-operating-model/vision.html

Where a recommendation rests on a principle, phrase it as a **preference between two real
competing goods**, with the cost visible. AWS's examples: *"We prioritize the many over the few…"*,
*"Speed matters: start small and iterate. We prioritize incremental delivery over extensive
analysis."* Order carries priority: *"The implied level of priority is from the first tenet to
the last one."* A tenet nobody could disagree with isn't a tenet.

---

## 4. Jargon — the main event

Four distinct classes. Only the first is what people usually mean by "jargon", and the third is
the one that actually degrades my replies.

### 4a. Domain jargon (specialist terms)
Google's definition (https://developers.google.com/style/jargon): *"specialized and often
figurative terminology of a specific group that represents a larger concept"* — plus *"vaguely
defined or overloaded terms like solution, support, or workload."* Why it's banned: *"Jargon can
hamper our efforts to publish content that's clear, that reaches a global audience in multiple
languages."*

Three sanctioned fixes:
1. **Replace with a specific term.** Google's own pairs: *blast radius* → *affected area /
   spatial impact*; *ingest* → *import / load*; *off-the-shelf* → *ready-made / pre-built*.
2. **Gloss on first use, then use it plainly.** Google: *"You then move the task to an earlier
   part of the process (also known as shifting left)."* And: *"The application is in the same
   state as a cold standby (a backup or redundant system that's identical to a primary system)."*
3. **Link to a trusted definition.**

**In this repo:** shorthand the user uses daily needs no gloss — silver/gold, parquet, the
firewall, the grain rules, `dt_page`, coverage. Glossing those is condescension, which Mailchimp
bans outright: *"Treat readers with the respect they deserve… don't patronize them."*

### 4b. Bloated Latinate vocabulary
Google (https://developers.google.com/style/translation): *"don't use words like commence when
you mean start or begin"*; *"don't use words like utilize or leverage when you mean use"*;
*"Don't use consequently when you mean so."*

| Use | Not |
|---|---|
| use | utilize, leverage, make use of |
| start, begin | commence, initiate, kick off |
| so | consequently, as a result, thus |
| to | in order to, for the purpose of |
| buy | purchase |
| because | since *(when you mean cause)* |
| tell | inform, advise, let you know |
| remove | eliminate, extract, strip out |
| about | approximately, on the order of |
| help | assist, facilitate, enable |
| connects to | establishes connectivity with |
| now | at this point in time |
| find | determine the location of |
| can | is able to, has the ability to |
| triggers | causes the triggering of |
| describes | provides a detailed description of |
| uses | makes use of |
| enough | sufficient |
| build | architect *(as a verb)* |
| change | modify, alter |
| show | demonstrate, illustrate |

Microsoft adds the weak-verb rule
(https://learn.microsoft.com/en-us/style-guide/word-choice/use-simple-words-concise-sentences):
avoid **be, have, make, do** and their padded forms; prefer the verb that carries the meaning.
Use *because*, not *since*, for causation: *"Because you created the table, you can change it."*

### 4c. Borrowed abstraction — my actual failure mode
Not covered by any of the source guides, because they address human writers. This is the class
the user is reacting to: words that add an abstraction layer over a plain claim and make it
sound more considered than it is.

| Say | Not |
|---|---|
| affects, changes | has implications for |
| shows | surfaces *(unless literally rendering to UI)* |
| the trade-off is | the tension here is |
| it costs more tokens | it's a token multiplier |
| the rule | the contract, the invariant *(unless it is one in code)* |
| a way to do X | a primitive, a mechanism, an abstraction over X |
| the limit | the envelope, the boundary |
| how it fits together | the shape of it, the topology |
| I'll check | I'll validate, I'll interrogate |
| the list | the surface, the space |
| this is hard to check | this is epistemically opaque |
| use it in more places | operationalize it, generalize it |

**Test:** would the sentence lose meaning if the word were replaced with the plain one? If not,
the word was decoration. Ubl's own doc models the discipline — it is written in plain sentences
about a subject that invites abstraction.

### 4d. Metaphor, idiom, and figurative language
Google tone (https://developers.google.com/style/tone) bans figurative language, humour, pop
culture, internet slang (*tl;dr*, *ymmv*), and *"wackiness, zaniness, and goofiness."*
Google translation bans idioms by name: *ballpark figure*, *back burner*, *hang in there*, plus
seasonal and geographic assumptions.

Google's inclusive-language substitutions are metaphor-removal as much as inclusion
(https://developers.google.com/style/inclusive-documentation):

| Use | Not |
|---|---|
| slows down the service | cripples the service |
| baffling outliers | crazy outliers |
| final check | sanity-check |
| doesn't respond | hangs |
| click | hit |
| person-hours | man-hours |
| humanity | mankind |
| allowlist / blocklist | whitelist / blacklist |
| controller, primary | master |
| replica, secondary | slave |
| placeholder | dummy |

Where a code identifier is literally `whitelist`, Google's pattern is prose-inclusive,
code-literal: *"Add a user to the allowlist (`whitelist`) by entering the following."*

### 4e. Acronyms and term consistency
Google TW1 (https://developers.google.com/tech-writing/one/words):
- Expand on first use with the acronym in parentheses — but **only introduce an acronym that is
  much shorter than the full term AND recurs many times.** In chat, that threshold is almost
  never met: expanding "API" for this user is noise.
- **One term per concept, always.** Never vary a term for stylistic relief. Google's example of
  the failure: introducing *Protocol Buffers* then switching to *protobufs* without linking them.
- **Replace a pronoun with its noun** when the referent is more than ~5 words back or a second
  noun intervenes. Google's unclear case: *"Python is interpreted, while C++ is compiled. It has
  an almost cult-like following."*

### 4f. The curse of knowledge
Google TW1 (https://developers.google.com/tech-writing/one/audience), verbatim: *"Experts often
suffer from the curse of knowledge, which means that their expert understanding of a topic ruins
their explanations to newcomers."*

The operative formula: write to the **gap** — what the reader needs minus what they already
have. In this repo the gap is rarely repo knowledge (the user has more than I do) and almost
always **the reasoning chain**: which query I ran, why that join is valid, what I checked and
what I didn't.

---

## 5. R3 — Claims about data

Bands and the weakest-link rule live in [evidence.md](../.claude/rules/evidence.md). This is only
how to *say* it. Sources: UK Government Analysis Function
(https://analysisfunction.civilservice.gov.uk/policy-store/communicating-quality-uncertainty-and-change/)
and the ONS service manual (https://service-manual.ons.gov.uk/content).

**Figure first, then significance, one sentence.** ONS: *"The UK unemployment rate was estimated
at 3.8%; it has not been lower since October to December 1974."* Semicolon to split if needed.

**One value per statement.** ONS: *"Include the most important value; avoid including multiple
values and percentages"* and *"Avoid overloading text with figures, percentages, and values."*

**Never open with a caveat.** ONS: *"avoid prefacing your main points with any introduction or
warnings. Do not introduce detailed definitions or quality warnings in your main points."*

**"Treat with caution" is banned.** The Analysis Function is explicit: *"Phrases like 'care must
be taken' and 'exercise caution' are not sufficient by themselves, because they do not help the
reader to understand what they can and cannot do with the numbers. Support statements like this
with practical advice."* ONS repeats it. Replace with two explicit lists — what the reader **can**
and **cannot** do with the number — each bullet starting in an active verb (*examine*, *compare*).

**Say "estimates" throughout**, not only in a methods aside. Analysis Function: *"Use words like
'estimates' throughout the publication, which helps to indicate that there is uncertainty around
the numbers."*

**Range first, central estimate second.** Analysis Function's DfT example: *"Final estimates for
2016 show that between 220 and 250 people were killed in accidents in Great Britain where at
least one driver or rider was over the drink-drive limit, with a central estimate of 230 deaths."*

**Name noise as noise.** Verbatim: *"This decrease is not statistically significant and it is
likely that the natural variation in the figures explains the change."* Use *broadly stable* for
movements too small to call.

**If the source can't support a trend, say the source can't support it** — don't soften the trend
instead. Verbatim: *"As offences involving the use of weapons are relatively low in volume, the
Crime Survey for England and Wales is not able to provide reliable trends for such incidents."*

**Round to the coarsest level still usable, and hold it.** ONS's three-audience example for one
value: public **260,000**; decision-makers **264,300**; analysts **264,337**. Watch their own
failure case: 436,254 rounded to millions renders as **"0 million"**.

> **Precedence — resolved 2026-07-20.** This collides with
> [evidence.md](../.claude/rules/evidence.md) §"Answer style": *"Report the number the query
> returned (38, not 'about 40'). Rounding is allowed only when flagged."* **evidence.md wins in
> chat.** The user sits in ONS's third tier (analyst), so exact-as-queried is the default;
> ONS coarsening applies only to lay-facing published copy, and is flagged when used. Without
> this clause both rules are always loaded and give different answers for the same figure.

**Drop the number if nothing changed.** ONS: *"If there is no noteworthy change in the data or
trend, do not include it."*

**Withhold the interpretive headline when the data can't carry it.** ONS: *"Do not use a
narrative title if there is no clear story to present, or where significant quality issues may
lead to misinterpretation."* State the figure alone.

**Text must add to a chart, not restate it.** ONS: accompanying text should *"add context and
detail to your charts; it should not repeat the trends shown in them."*

**Causal vocabulary is a claim, not a description.** ONS words-to-watch: *due to* → *because of*
or *owing to*; *since* (as cause) → *because*; *compared to* → *compared with*. Writing "the
effect of X on Y" asserts causation. Neither ONS nor the Analysis Function states an explicit
correlation-vs-causation rule — that gap is filled here by `evidence.md`'s weakest-link rule and
the repo's standing ban on inference in UI copy.

**Never let "important" or "interesting" stand alone.** ONS: *"Specify why and to whom."*

---

## 6. Formatting

**Prose is the default; bullets are the exception.** The justification is Amazon's (§3): bullets
hide the connective tissue between claims. Use a list only when items are genuinely parallel and
unordered, or when a sentence already contains an embedded list — Google TW1's trigger case:
a sentence with an *either/or* or a run of *and*s becomes a list.

**Number a list only when reordering changes the meaning** (Google TW1). Otherwise bullet it.

**Keep list items parallel** in grammar, category, capitalisation, and punctuation (Google TW1).

**A heading needs at least two sections under it.** One heading is a label, not structure.

**Sentence case for headings; serial comma.** Google and Microsoft agree on both.

**Markdown links for file references**, per CLAUDE.md — never backticks or HTML.

**Avoid single-tilde strikethrough** — use ≈ (existing repo memory).

---

## 7. Sentence-level, always on

**Cut these outright:**
*simply, easy, easily, quickly, just, obviously, of course, essentially, basically, actually,
very, quite, really, please note, at this time, it's worth noting, it's important to note,
powerful, seamless, a variety of, in order to.*

Google tone bans *simply / it's easy / quickly* as condescension in a procedure; Microsoft bans
the adverbs as filler — *"quite very quickly easily effectively."* Same words, two independent
reasons.

**Two neighbouring classes belong to [evidence.md](../.claude/rules/evidence.md), not here** —
**hedges** and **unfalsifiable filler** — both under its §"Answer style". Deliberately not
restated: three divergent copies of the hedge list existed before 2026-07-20, and a partial
restatement reads as a complete one. The remedies differ and that difference is load-bearing:
filler is **replaced with the measure it lacks**; the words above are **deleted**.

**One idea per sentence.** Split when a clause introduces a second idea. Google TW1 flags the
trigger words: *which, that, because, whose, until, unless, since*.

**Kill "there is / there are."** Google TW1: *"There is a variable called met_trick that stores
the current accuracy"* → *"The met_trick variable stores the current accuracy."*

**Strong verb, not weak-verb-plus-noun.** *"The exception occurs when dividing by zero"* →
*"Dividing by zero raises the exception."*

**Active voice.** Detection test (Google TW1): a form of *be* + past participle, often followed
by *by*. *"A wrapper is generated by the Op registration process"* → *"The Op registration
process generates a wrapper."*

**Cut the subjective adjective; give the number** — *"runs screamingly fast"* → *"runs 225–250%
faster."* **Caveat that overrides the rule:** only when the number is one I actually measured.
Inventing precision to satisfy this rule is worse than the adjective, and `evidence.md` wins.

**No double negatives.** *"You cannot not invoke this flag"* → *"You must invoke this flag."*

**"Only" goes directly before what it limits.** *"Request only one token"*, not *"Only request
one token."*

**Don't stack modifiers.** *"A hybrid cloud-native DevSecOps pipeline"* → *"A cloud-native
DevSecOps pipeline in a hybrid environment."*

**Contractions are fine.** Microsoft mandates them; Google is silent. (See §9.)

**Front-load.** Lead with the answer; assume only the first screen is read (Google TW1).

**First sentence of a paragraph carries its point** (Google TW1).

### The editing pass — non-negotiable
- **Delete a random sentence. Did it matter?** If not, leave it deleted. (37signals, verbatim:
  *"Occasionally pick random words, sentences, or paragraphs and hit delete. Did it matter?"*)
- **Read it as the user** — someone with full repo context who doesn't need the setup.
- **Check the register.** Did an R0 question get an R2 answer?
- **Check the first line.** Does it contain the answer, or announce that an answer is coming?
- **Don't over-cut.** Mailchimp: *"Write briefly, but don't sacrifice clarity for brevity."*
  Google's error-message guidance names the failure mode: cutting *"Unable to establish
  connection to the SQL database"* down to *"Unsupported"* is worse, not better.

---

## 8. What is mechanically checkable

The enforcement gradation, which mirrors
[feedback_guardrail_determinism_tiers](../MEMORY.md). Determinism is inverse to consequence:
the cheap mechanical checks can be hard-enforced; the high-value semantic ones must not be.

**Lintable (a Stop hook can catch these):**
- banned-word list (§7) and hedge list
- the §4b and §4c substitution tables
- "treat with caution" / "care must be taken" / "exercise caution"
- sentence length over ~40 words
- single-tilde strikethrough
- a heading with only one section under it
- closing filler ("let me know if", "hope this helps")

**Not lintable — instruction only:**
- register choice
- whether a non-goal is a real non-goal
- whether an alternative's trade-off was actually named
- whether a figure's caveat is the *right* caveat
- whether a term is load-bearing jargon or decoration
- whether the reasoning chain has a gap

Hardening the second list into pattern-matching would produce false positives on exactly the
answers worth writing. Leave it as instruction and accept the drift.

---

## 9. Where the sources disagree

Recorded rather than resolved, so a future edit doesn't "fix" one by breaking the other.

**Contractions.** Microsoft mandates them (*"Use contractions like it's, you'll, you're, we're,
and let's"*). Google's tone page bans *let's* outright and never mentions contractions. Mailchimp
contradicts itself: *"Feel free to use contractions"* in its TL;DR, *"Use contractions with
caution"* in its translation section. **Resolution here:** contractions yes, *let's* no.

**Humour and rule-breaking.** Google bans it flatly. Mailchimp invites it: *"Don't be afraid to
break a few rules if it makes your writing more relatable."* Microsoft sits between.
**Resolution here:** Google's position, because this repo's output is publication-grade civic
data.

**Heading capitalisation.** Mailchimp uses title case for buttons; Microsoft and Google both use
sentence case. **Resolution:** sentence case.

**Em-dash spacing.** Microsoft says no spaces (*"pipelines—logical groups of activities—to"*).
Google and Mailchimp are silent. It's a house rule, not a consensus, and isn't enforced here.

**One-sentence paragraphs.** Google TW1 says *"Avoid one-sentence paragraphs"* — correct for
reference docs, actively wrong for chat, where a one-line answer is often the right answer. This
repo overrides Google here.

---

## 10. Known gaps

- **The GOV.UK / plainlanguage.gov / 18F / Home Office cluster was not read.** Its likely
  contribution — sentence length of 15–20 words (max ~25), a words-to-avoid list, and the
  research finding that high-literacy readers prefer plain English *more* than low-literacy
  readers — is known only from search snippets, not from the pages. **[Reported — search result,
  not fetched.]** Fetch before relying on the numbers.
- **Google's full A–Z word list** (https://developers.google.com/style/word-list) was never
  fetched. The three jargon pairs in §4a are only those surfaced on the jargon page itself.
- **The Analysis Function uncertainty page** rendered as a condensed version; the quoted example
  sentences are verbatim, but its PDF carries fuller example boxes that were not surfaced.
- **The Economist style guide** is print-only; no free source exists to check against.
