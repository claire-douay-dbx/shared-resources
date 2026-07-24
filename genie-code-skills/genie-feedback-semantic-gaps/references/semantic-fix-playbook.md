# Semantic fix playbook — gap type → concrete Genie remedy

How to turn each ranked gap group into a **concrete, drafted** semantic fix for the Genie room.
**Guidance only — this skill drafts fixes; it never applies them to the agent config.**

## The Genie semantic levers (what you can draft)

A Genie room's answer quality is governed by a small set of semantic assets. Every fix is one or
more of these:

1. **General instructions** — natural-language rules the agent always follows (business
   definitions, default assumptions, "always/never" rules).
2. **SQL examples (example queries)** — curated question→correct-SQL pairs the agent learns from.
3. **Synonyms / value dictionary** — map the words users actually type to the right columns/values.
4. **Join specifications** — the correct join path(s) between tables.
5. **Table & column comments (metadata)** — descriptions that remove ambiguity at the schema level.

## Mapping — gap type → remedy

| Gap type | Primary remedy | Secondary | Draft looks like |
|---|---|---|---|
| **Wrong table** | Instruction naming the authoritative table | Table comment | "For revenue always use `sales.orders` (not `sales.orders_raw`)." |
| **Wrong / missing filter** | Instruction encoding the business rule + example SQL | Synonym on the metric name | "Revenue excludes refunded/cancelled orders: filter `order_status NOT IN ('refunded','cancelled')`." |
| **Missing / wrong join** | Join spec | Example SQL showing the join | join: `fact_orders.region_id = dim_region.id` |
| **Terminology misread** | Synonym / value-dictionary entry + instruction | Column comment | "'active user' = user with ≥1 session in period → `fact_sessions`." |
| **Ambiguous question handling** | Instruction on default assumption | Example SQL | "If no period is given, default to the last full calendar month." |
| **Missing metric / column** | Metric definition instruction (+ flag if column truly absent) | Example SQL | define the metric formula and source |
| **Wrong grain** | Instruction on default grain + example SQL at the right grain | — | "Default revenue grain is month unless the user asks otherwise." |
| **Calculation error** | Pin the formula in instructions + example SQL | Column comment | "Conversion rate = orders / sessions, both in the same period." |
| **Hallucinated object** | Table/column comments so the real schema is described | Instruction listing valid objects | describe the actual tables/columns available |

## Drafting rules

- **Be specific and paste-ready.** A developer should be able to copy the drafted instruction /
  example SQL / synonym straight into the agent (after review). Vague advice ("improve the
  revenue definition") is not a fix.
- **Prefer the lightest lever that fixes it.** A synonym or one instruction often beats a big
  example-SQL dump. Reserve example SQL for recurring, non-trivial queries.
- **Example SQL is always `-- DRAFT — verify against schema`.** Observed/generated SQL can be
  truncated or reflect a one-off phrasing; the fix SQL is a starting point, not ground truth.
- **Tie the fix to the evidence.** Cite the conversation(s) and the comment text that motivated it,
  so the developer can judge it.
- **One group → one coherent fix** (which may combine levers). Don't scatter a single root cause
  across several half-fixes.

## Protect + replicate (from the positive groups)

- **Protect:** for each win group, write a one-line note that the semantics behind it (the specific
  table/join/filter/instruction that makes it work) must **not regress** when other fixes land —
  e.g. a new instruction shouldn't override a rule that's currently producing 👍 answers.
- **Replicate:** when a gap group is adjacent to a win group, reuse the win's winning pattern as the
  fix template. Example: "orders by day" gets 👍 with a clean `date_trunc('day', ...)` example — reuse
  that exact pattern to fix a "revenue by day" group that's currently answered at the wrong grain.

## Prioritisation reminder (frequency + severity)

The skill ranks gaps by **frequency first, severity as tiebreak/lifter**, showing both numbers.
The playbook doesn't change the ranking — it fills in *what the fix is* for each ranked group. Order
the drafted developer actions to match the ranking so the highest-impact fix is done first.

## Going beyond 7 days (programmatic path — optional, not this skill's default)

Analyze Space Usage and the digest are fixed to the **last 7 days**. For a longer window, feedback
*events* can be reconstructed programmatically — but with a hard limit:

- `system.access.audit` (`service_name = 'aibiGenie'`) records the **👎 rating event**
  (`updateConversationMessageFeedback` → `request_params.feedback_rating`) and the **review-request
  event** (`createConversationMessageComment`), with `space_id` / `conversation_id` / `message_id`,
  user, and timestamp — at scale, for any window.
- The **Genie Conversation API** then supplies the question + generated SQL for those message IDs.
- **But comment TEXT is UI-only** — it is *not* in the audit log and *not* in the Conversation API.
  So a programmatic run can tell you *which* messages got 👎 / review requests and show their SQL,
  but **not why the user complained**. The comment "why" only exists in the UI path this skill uses.

The sibling `genie-weekly-monitor` skill implements this audit + Conversation API pattern for
problem-interaction RCA; reuse its query/endpoint reference if a longer window is needed, and state
the comment-text limitation in the output. Keep its guardrails: rate-limit backoff, an enrichment
cap, and no silent truncation.
