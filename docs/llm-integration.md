# LLM integration: stateless calls, tokens, structured output, lightweight RAG

The LLM learning topics from `technology_stack_topics.md`, tied to how `ai-incident-responder-service`
calls Claude. Code: `ai/ClaudeIncidentPlanner.java` (the request), `ai/IncidentPromptBuilder.java`
(system prompt + RAG context), `ai/IncidentPlan.java` (the output schema),
`repository/{MetricHistoryRepository,GraphRepository}.java` (the two retrieval sources).

**Status (2026-10-25):** the pipeline is verified end-to-end in mock mode (`AI_CLAUDE_MOCK=true`,
`MockIncidentPlanner`). The real Claude call and the count-tokens test (`./gradlew promptTokens`)
have not been run yet — both need `ANTHROPIC_API_KEY`. The token numbers in §2 are estimates until
that test runs; replace them with its output.

## 1. The stateless nature of the API

The Messages API keeps **no state between requests**. There is no session on the server: the model
sees exactly what one request contains — `system`, `messages[]`, tool/schema definitions — and
nothing else. A "conversation" in a chat app is the client re-sending the whole history every turn,
and paying for all of it every turn.

Consequences, and what this service does about each:

| Consequence | Here |
|---|---|
| All context must be in the request | Each alert's request is self-contained: the system prompt, the alert, the 1-hour metric summary and the impacted objects (`IncidentPromptBuilder.build`) |
| History costs tokens on every turn | No history: **one user message per alert** (`ClaudeIncidentPlannerTest` asserts `messages().size() == 1`). Alerts are independent, so a chat-style history would only add cost and cross-talk between unrelated incidents |
| Any replica can serve any request | The worker holds no per-incident memory; idempotency lives in the DB (`UNIQUE(alert_id)`), so Kafka can rebalance alerts between replicas freely |
| Retries are safe to repeat | Re-sending the same request is a fresh, independent call. The SDK retries 408/429/5xx (2× by default); the Resilience4j circuit breaker (`ResilienceConfig`) fails fast while Claude is down, and the Kafka error handler redelivers the alert later |
| Output is not deterministic | The same alert can get differently-worded plans. Idempotency (`ON CONFLICT (alert_id) DO NOTHING`) keeps the **first** plan, so a redelivered alert never overwrites a plan the crew already saw |

## 2. Tokens, tokenization, context window

**Tokens** are the unit the model reads, writes and bills by — sub-word pieces produced by the
model's **tokenizer** (a BPE-style vocabulary). English text is roughly 3–4 characters per token;
IDs, numbers and punctuation-heavy text (`bld-12`, `2026-09-06T10:05:00Z`) cost more per character.
Token counts are tokenizer-specific: the same prompt counts differently on different model
generations, so measure on the target model with the count-tokens endpoint, never with a
third-party tokenizer such as `tiktoken`.

The **context window** is the maximum tokens one request can hold: input + output (including
thinking). `claude-opus-5-5` has a 1M-token window and up to 128K output. The relevant limits here:

| Limit | Value | Where |
|---|---|---|
| Context window | 1M tokens | model property |
| `max_tokens` (output ceiling: thinking + the plan) | 16 000 | `ClaudeIncidentPlanner.MAX_TOKENS` — adaptive thinking draws from the same ceiling, so a tight value risks the plan being cut off mid-JSON (`stop_reason=max_tokens`) |
| Thinking depth | `effort: high` | `ai.claude.effort` — the main lever on output-token spend; Opus 5.5 defaults to `medium` |
| Input side of the prompt | bounded by construction, ~1–2k tokens (estimate) | `IncidentPromptBuilder` |

With a 1M window the prompt cannot realistically overflow, so here **context management is about
cost, latency and signal-to-noise, not fit**. Every input token is billed on every alert
(`$4/MTok` input, `$20/MTok` output on Opus 5.5), and an unbounded list of 2 000 buildings would
dilute the context without improving the plan. How the prompt is bounded:

1. **Metrics are summarised, not dumped.** The last hour can be thousands of readings at the
   simulator's rate; the prompt carries a fixed six-field summary (count, min, max, avg, latest,
   latest-at) from one SQL aggregate (`MetricHistoryRepository.findLastHour`). Constant size
   regardless of ingestion volume.
2. **The graph walk is bounded twice.** Depth: `GraphRepository.HIERARCHY_DEPTH = 3` hops (the
   longest service chain is 2). Breadth: at most `IncidentPromptBuilder.MAX_IMPACTED_IN_PROMPT = 50`
   objects are listed; the rest are **counted, not silently dropped** (`- ... and 70 more downstream
   objects (120 in total; list truncated to fit the prompt)`), so the model still knows the blast
   radius. Crews are capped the same way: at most `MAX_CREW_ASSETS_IN_PROMPT = 10` assets per crew,
   the rest counted.
5. **The cheapest call is the one not made.** Root-cause correlation over the graph files an alert
   under an active upstream incident without calling Claude (`model = "correlated"`): one substation
   outage with 31 downstream alerts costs one plan, not 32. See
   [`system-overview.md`](system-overview.md#граф-города-и-корреляция-инцидентов).
3. **Measured, not guessed.** `PromptTokenBudgetTest` (tag `llm`, `./gradlew promptTokens`, excluded
   from `build`) sends the exact request — system prompt, messages, thinking config and the
   `IncidentPlan` schema — to `POST /v1/messages/count_tokens`, which tokenizes on the target model
   without generating anything. It prints the cost of each prompt part and asserts that 2 000
   impacted objects cost about the same as 50 and stay under a 4 000-token input budget.
4. **Observed in production.** Every real call logs `response.usage()`:
   `input_tokens`, `output_tokens`, `cache_read`, `cache_write` per alert.

**Prompt caching** (re-using an identical request prefix at ~5% of the input price) is not enabled:
the stable prefix — system prompt + schema — is below Opus 5.5's 512-token minimum cacheable
prefix (to confirm with the count-tokens output), so a cache marker would be silently ignored. It
becomes worthwhile if the system prompt grows, e.g. with a runbook of repair procedures. The system
prompt is already a separate, byte-identical constant so that change would be a one-line
`cacheControl` addition.

## 3. Structured prompt engineering: system prompt + guaranteed JSON

### System prompt

The request has two channels with different roles:

- **`system`** — operator instructions: who the model is and the rules it follows.
  `IncidentPromptBuilder.SYSTEM_PROMPT`: incident-response coordinator for the city network; use
  only the given context and don't invent objects; criticality is exactly `HIGH`/`MEDIUM`/`LOW` and
  when to rate higher; one-sentence summary; ordered, concrete steps starting with making the site
  safe; address the steps to the listed crews and don't invent crews. Identical on every request.
- **`messages[0]` (user)** — the data for this alert: the alert fields, the metric summary, the
  impacted objects, the responsible crews.

Keeping instructions out of the data channel matters for two reasons: instructions in `system` are
treated as the operator's, so text inside the data (e.g. a sensor or building name) is less likely
to be read as an instruction; and a fixed system prompt is a stable, cacheable prefix while the
user message varies per alert.

### Forcing valid JSON without filler

Three generations of technique, and why this service uses the last:

| Technique | How | Guarantee |
|---|---|---|
| Prompting ("reply only with JSON…") + parsing | Instructions + `JSON.parse` on the text | None. The model can add a preamble ("Here is the plan:"), wrap it in a code fence, or emit invalid JSON; the client needs repair/retry logic |
| JSON mode / tool calling | Define a tool whose `input_schema` is the plan and make the model call it; read `tool_use.input` | Valid JSON, and with `strict: true` schema-valid arguments. But it bends the tool mechanism into an output format, and on Opus 5.5 forced `tool_choice` (`any`/`tool`) is rejected with a 400, so the model can't be made to call it |
| **Structured outputs** (`output_config.format`) | Send a JSON Schema as the response format | **Constrained decoding**: the response text is generated to match the schema, so it is always parseable and schema-valid. No preamble, no fences, no repair |

`ClaudeIncidentPlanner.buildParams` uses the Java SDK's `.outputConfig(IncidentPlan.class)`: the
SDK derives the JSON Schema from the `IncidentPlan` record (field names, types, and the
`@JsonPropertyDescription` texts the model sees as field docs), sends it as `output_config.format`,
and deserializes the reply straight into an `IncidentPlan`. The response handling never touches raw
JSON. Two edge cases remain and are handled explicitly:

- **Refusal** — a safety-classifier decline is HTTP 200 with `stop_reason: refusal` and no plan;
  `plan()` checks it before reading content and throws a clear error (→ retry / DLQ).
- **`max_tokens`** — output cut off mid-JSON would fail to parse; `MAX_TOKENS = 16 000` leaves room
  for thinking plus the plan.

The schema constrains **shape**, not **content**: `criticality` is a `String`, so "exactly one of
HIGH/MEDIUM/LOW" is enforced by the description and the system prompt, not by the decoder. An
`enum` type would make it hard-guaranteed.

## 4. Lightweight RAG: context from TimescaleDB + AGE

**Retrieval-Augmented Generation**: before calling the model, retrieve the facts it needs and put
them in the prompt, so the answer is grounded in current data the model was never trained on. Here
the retrieval is "lightweight": no embeddings or vector store, but two **exact, structured
queries** keyed by the alert's `sensor_id` — the right fit when the relevant data is addressable by
key rather than by semantic similarity.

```
infrastructure-alerts ── {alert_id, sensor_id, metric_type, value, rule, detected_at}
        │
        ├─► TimescaleDB  MetricHistoryRepository.findLastHour(sensor_id, metric_type)
        │      one aggregate over `metrics`: count/min/max/avg + latest  →  6 numbers
        │
        ├─► AGE          GraphRepository.findMonitoredAsset(sensor_id)       (Sensor)-[:SensorLink]->(asset)
        │                GraphRepository findImpact(asset), one hybrid statement:
        │                  cypher((asset)-[*0..3]->(d)) LEFT JOIN crew_assignments / repair_crews
        │                GraphRepository findUpstream(asset)                 (asset)<-[*1..3]-(u)
        │      Cypher in SQL, bound as ?::agtype  →  [label, id, name] per object + crews
        │
        ├─► TimescaleDB  IncidentRepository.findActiveRoot(upstream ids, detected_at ± 30 min)
        │      found → child incident, no Claude call (stop here)
        ▼
IncidentPromptBuilder  system prompt  +  "## Alert" / "## Last hour of readings" / "## Impacted objects"
                                       / "## Responsible crews"
        ▼
Claude (structured output: IncidentPlan)  →  incidents table (+ crews from the graph, not the model)
```

What makes it fit the token budget (see §2): **aggregate before prompting** (statistics instead of
raw series), **project only what the model needs** (label, id, name, not every vertex property),
**cap and count** the graph list, and **say when a source is empty** ("no readings recorded in the
last hour", "none resolved") so the model doesn't guess. The two sources answer different
questions — *how bad is it* (the time series) and *who is affected* (the graph) — and the plan needs
both: criticality depends on the reading's trend and on how many residential buildings lose
service.

The mock planner (`MockIncidentPlanner`) consumes the same retrieved context, so the RAG half is
verified end-to-end without a key: 3 faults → 3 alerts → 3 incidents with impacted objects resolved
from the graph, and with crews and correlation since 2026-11-08 (`CorrelationIntegrationTest` on
real AGE + TimescaleDB). The crews section changed the prompt, so the §2 token figures are still
estimates until `promptTokens` runs.

## Running the token measurement

```bash
cd ai-incident-responder-service
export ANTHROPIC_API_KEY=...          # count_tokens is free, but needs a key
./gradlew promptTokens                # prints tokens per prompt part; no billed generation
```
