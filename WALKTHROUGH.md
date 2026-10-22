# Walkthrough

This is a guided tour for explaining Beacon in an interview: what each part does and
why it was built that way, then one real incident traced end to end with the actual
values, then the questions you're likely to get and how to answer them.

---

## Part 1 — The components, in plain English

### The pretend data platform
**What it is:** Airflow running five small pipelines (sales ETL, event ingest, feature build,
report export, metrics rollup) against a Postgres "warehouse", plus a fake external API
(`upstream-mock`) that one pipeline depends on.
**Why:** Beacon needs something real to watch. The pipelines are tiny, but each one fails the
way real pipelines fail, and each one logs the evidence a human would look for. The
failures can be switched on and off with Airflow Variables, so no code is ever edited to
break things.
**Why LocalExecutor:** tasks run inside the scheduler container. That means fewer
containers and no Celery/Redis, and stopping the scheduler genuinely stops everything,
which makes the "scheduler down" scenario honest.

### Chaos CLI
**What it does:** `beacon-chaos inject <scenario>` breaks the platform in one of eight ways
and writes down the right answer (the "ground truth") in a `fault_injections` table.
`reset` puts everything back.
**Why:** you can't measure a diagnosis tool without knowing the right answer. Recording the
ground truth at injection time is what makes the evaluation possible.

### Monitoring: statsd-exporter → Prometheus → Alertmanager
**What it does:** Airflow emits metrics. A mapping file turns them into clean Prometheus
series with `dag_id`/`task_id` labels. Five alert rules decide when something is wrong, and
Alertmanager posts a webhook to Beacon.
**Why this way:** it's the standard open-source stack, and Beacon stays decoupled. Anything
that can send an Alertmanager webhook can start an investigation.
**A detail worth mentioning:** `increase()` can't see the first-ever increment of a new
counter series, so the first failure of a DAG would never alert. The rules add an
`unless … offset 5m` branch to catch series that didn't exist five minutes ago.

### beacon-api
**What it does:** receives alerts and turns them into incidents. It deduplicates them, runs
the agent in the background, stores the diagnosis, and posts it to Slack. It also serves
`/ask`, `/status`, `/actions/rerun`, the incident list, feedback and `/metrics`.
**Why dedupe on fingerprint + run id:** Alertmanager re-sends a firing alert every minute,
and two rules (`DagRunFailed`, `TaskFailureSpike`) can fire for the same broken run. One
failed run should produce one incident, but a *new* failed run should produce a new
incident even while the alert is still firing.

### The agent
**What it does:** an LLM in a loop. It can call seven read-only tools (platform status, recent
failures, run details, task logs, PromQL, runbook search, read runbook), sees the results,
and keeps going until it can answer. The answer must be JSON matching the `Diagnosis` model:
summary, user impact, root cause, confidence, evidence, runbook id and suggested action.
**Guard rails:**
- At most 8 tool rounds, with a per-tool timeout.
- Tool arguments are validated against a schema, and errors go back to the model so it can
  fix them.
- One retry if the final JSON is invalid, otherwise an explicit "unknown".
- Secrets are masked before anything reaches the model.
- The prompt tells it to quote evidence and to say "unknown" rather than guess.

**Why it's read-only:** see Part 3.

### Runbook retrieval (CRAG)
**What it does:** embeds the 12 runbooks with MiniLM, looks up the top k in FAISS, and grades
each one. If none is good enough, it rewrites the query from the error signals and tries
once more. It returns only runbooks that pass.
**Why:** plain top-k retrieval always returns *something*. Grading plus one corrective
rewrite means the agent either gets a relevant runbook or is told there isn't one. Example:
the bare log line `KeyError: 'unit_price_usd'` scores 0.31 on the first pass (below the 0.45
threshold), gets rewritten to "Python exception traceback code bug KeyError", and then finds
`rb-code-error`.

### Slack bot
**What it does:**
- `/platform-status`, `/why <dag> [run]` and `/runbook <query>`.
- Buttons on each incident post: Rerun, Acknowledge, Show evidence, 👍/👎.

It runs in Socket Mode, so no public URL is needed.
**Why `/platform-status` and not `/status`:** Slack reserves `/status` for itself.

### MCP server
**What it does:** exposes the same read-only tools and the runbooks to any MCP client (Claude
Code, Claude Desktop) over stdio or HTTP.
**Why:** the investigation tools are useful on their own, for an engineer poking around
interactively. Because the MCP server and the agent share `beacon/tools.py`, there's exactly
one implementation of each tool, with the same validation and masking.

### Grafana, Pulse, evaluation
- **Grafana:** two provisioned dashboards. *Platform Health* is for pipeline owners ("Are my
  pipelines healthy?"); *Support & Incidents* shows Beacon's own performance.
- **Pulse:** every agent run becomes a trace, with a span per LLM call and per tool call.
- **beacon-eval:** injects random scenarios, scores the agent and the regex baseline against
  ground truth, and reports accuracy, a confusion matrix and latencies.

---

## Part 2 — One real incident, end to end

These are the actual values from incident **#20** (scenario `missing_input`), captured from
the running stack.

**1. Injection (18:58:02 UTC).**
```
$ docker compose run --rm tools beacon-chaos inject missing_input
injected missing_input (fault_injection id=12, ground truth=missing_upstream_data)
triggered sales_daily_etl run manual__2026-10-22T18:58:02.014023+00:00
```
Inside the scheduler container, `incoming/sales.csv` was moved to `.quarantine/`.

**2. The pipeline fails (18:58:04 → 18:58:19).** `extract` fails, retries after 10 s, and fails
again. `clean`, `aggregate` and `load` become `upstream_failed`. The run lasts 15.5 s.

**3. The alert (startsAt 18:58:32).** About 13 s later Prometheus' rule evaluation fires, and
Alertmanager holds it for `group_wait` (5 s) before sending the webhook. The alert as
Alertmanager reported it:
```json
{
  "labels": {"alertname": "DagRunFailed", "dag_id": "sales_daily_etl", "severity": "page"},
  "annotations": {"summary": "DAG sales_daily_etl had a failed run in the last 5 minutes"},
  "startsAt": "2026-10-22T18:58:32.145Z",
  "fingerprint": "1364840253b4c84e"
}
```

**4. Incident created (18:58:39).** beacon-api asks Airflow for the latest failed run of
`sales_daily_etl`. That gives run `manual__2026-10-22T18:58:02.014023+00:00` and failed task
`extract`. It checks the dedupe key (fingerprint `1364840253b4c84e` + run id), finds no
existing incident, and links fault injection 12 because the failure falls inside its time
window. It saves incident #20 as `open` and starts the agent. The agent's first message:
```
Alert DagRunFailed is firing (started 2026-10-22T18:58:32.145Z).
Alert summary: DAG sales_daily_etl had a failed run in the last 5 minutes
Labels: dag_id=sales_daily_etl, severity=page
Most recent affected run: dag_id=sales_daily_etl run_id=manual__2026-10-22T18:58:02.014023+00:00; failed task(s): extract
Investigate and diagnose the root cause.
```

**5. The investigation (`gemini-3.5-flash-lite`, 5 LLM rounds, 46 s, 11.6k tokens).**

| Step | Tool call | What came back |
|---|---|---|
| 1 | `get_dag_run(dag_id="sales_daily_etl", run_id="manual__…018023…")` | state `failed`, 15.5 s; `extract` failed on try 2, the rest `upstream_failed` (353 ms) |
| 2 | `get_task_logs(dag_id="sales_daily_etl", run_id=…, task_id="extract")` | `Reading upstream sales drop from /opt/airflow/data/incoming/sales.csv` → `Input file … does not exist. Files present in /opt/airflow/data/incoming: []` → `FileNotFoundError: Expected input file not found: …` (867 ms) |
| 3 | `search_runbooks(query="FileNotFoundError")` | `rb-missing-input`, score 0.634, the only runbook above the threshold (130 ms) |
| 4 | `get_runbook(runbook_id="rb-missing-input")` | full runbook text (1 ms) |
| 5 | final answer | JSON below |

**6. The diagnosis.** It validated against `Diagnosis` on the first try:
```json
{
  "summary": "The daily sales pipeline failed because the expected input file (/opt/airflow/data/incoming/sales.csv) was missing from the incoming directory. As a result, the extract task and all downstream steps could not proceed.",
  "user_impact": "The sales_daily_etl pipeline failed, preventing the daily sales report and downstream analytics from being updated.",
  "root_cause": "missing_upstream_data",
  "confidence": 1.0,
  "evidence": [
    "FileNotFoundError: Expected input file not found: /opt/airflow/data/incoming/sales.csv",
    "Input file /opt/airflow/data/incoming/sales.csv does not exist. Files present in /opt/airflow/data/incoming: []"
  ],
  "runbook_id": "rb-missing-input",
  "suggested_action": "Verify that the upstream data producer has delivered the sales.csv file to /opt/airflow/data/incoming/ or re-deliver it, then retry the pipeline."
}
```
Both the root cause and the runbook match the ground truth.

**7. The Slack post (18:59:26, `ts=1792695566.676689`).** It went up 47 s after the alert
arrived and 85 s after the injection:
```
🔴 sales_daily_etl: pipeline run failed
The daily sales pipeline failed because the expected input file (/opt/airflow/data/incoming/sales.csv)
was missing from the incoming directory. As a result, the extract task and all downstream steps could not proceed.

Who is affected: The sales_daily_etl pipeline failed, preventing the daily sales report and downstream
analytics from being updated.            |  Likely cause: Missing input data `missing_upstream_data` (100% confidence)
Evidence (2): `FileNotFoundError: Expected input file not found: …` — click Show evidence for all of it
Runbook: Expected input file or upstream data is missing (`rb-missing-input`)
  1. Ask the upstream producer to (re)deliver the file to the expected path, or copy it from its archive location.
  2. Once the file is present, rerun the extract task; downstream tasks will follow.
  3. If late deliveries are common, add a FileSensor in front of the extract task instead of failing outright.
Next step: Verify that the upstream data producer has delivered the sales.csv file … then retry the pipeline.
Incident #20 · alert `DagRunFailed` · run `manual__2026-10-22T18:58:02.014023+00:00` · task `extract`
[Rerun task] [Acknowledge] [Show evidence] [Helpful 👍] [Not helpful 👎]
```

**8. Tracing.** Pulse shows the trace `incident-investigation`: status success, 46.2 s,
11,601 tokens. There's a root `agent` span with child `llm` and `tool` spans.

**9. Afterwards.** `beacon-chaos reset` restores the file. Clicking **Rerun task** opens a
confirmation modal. If you're on `RERUN_ALLOWED_USERS`, beacon-api clears `extract` and its
downstream tasks and the result is posted in the thread. Five minutes after the last failure,
`DagRunFailed` resolves. The original post then becomes "✅ [RESOLVED]", loses the rerun
button, and gets a thread reply.

---

## Part 3 — Likely interview questions

**"How do you stop the agent from doing something dangerous?"**
Several independent layers:
1. **Capability.** The agent is only ever handed read-only tools. `rerun_task` isn't
   registered with it or with the MCP server, and a test asserts that.
2. **Human gate.** A rerun needs a button click, a confirmation modal and an allowlisted
   user. The allowlist is checked in the bot *and* again in the API, so a hand-crafted
   request can't skip it.
3. **Bounded tools.** PromQL must be a single plain expression with capped range, offset and
   result size. There is no tool that runs shell commands.
4. **Data hygiene.** Secrets are masked before anything goes to the LLM or Slack.
5. **Bounded loop.** At most 8 rounds, per-tool timeouts, and a schema-validated output that
   falls back to "unknown" instead of guessing.

It's the same principle as UnderCurrent's state machine: the model proposes, and only an
authorised actor can make the transition.

**"How do you know it's accurate?"**
Ground truth is recorded at injection time, and `beacon-eval` runs 30+ random scenarios
end to end through the real alerting path. It scores root cause *and* runbook against that
ground truth and compares with a regex baseline. The README has the real numbers, the
confusion matrix and every miss with the agent's evidence and tool calls. In production
you'd add the 👍/👎 feedback rate (already on the dashboard) and periodic human review of a
sample of incidents.

**"Why not just use the regex baseline?"**
On clean synthetic failures the baseline does very well, and saying so is the honest
result. The agent does the rest of the job:
- it finds the failing run and task itself
- it reads the right log
- it writes a summary a pipeline owner understands
- it picks the runbook
- it handles messages and situations the regex list never anticipated, and says "unknown"
  when it can't tell

The baseline is also a useful regression floor.

**"Why a free-tier LLM? Isn't that a toy?"**
It forces discipline that also matters at scale:
- short prompts, and logs cleaned and cut to the relevant tail
- library traceback frames collapsed
- the run id handed to the agent so it doesn't waste steps finding it
- 429 handling that honours the provider's retry delay (header or Gemini's `retryDelay`)

Switching to Groq, Gemini, Ollama or a paid model is a `.env` change.

**"What happens when the LLM or Airflow is down?"**
- Every tool returns a clear error instead of raising.
- If the agent can't run, the incident still gets posted with an "unknown — investigate
  manually" diagnosis and the error recorded.
- The Slack bot answers every command, saying plainly that Airflow or the model is
  unavailable.
- Tracing to Pulse fails open.

**"What would you change at Apple's scale?"**
- **Throughput and isolation:** a queue (Kafka/SQS) between the webhook and a pool of agent
  workers, instead of in-process tasks with a semaphore. Also idempotency keys, and a
  durable store for the dedupe state.
- **Correlation before investigation:** at scale one upstream outage breaks hundreds of DAGs.
  You'd group alerts by shared dependency/lineage first and investigate the cluster once.
- **Retrieval:** thousands of runbooks and past incidents in a proper vector store with
  metadata filters (team, service). Past resolved incidents become retrieval sources too.
- **Evaluation:** a continuously growing labelled set from real incidents (feedback +
  postmortems), offline regression tests on every prompt/model change, and canarying a new
  model on a slice of traffic.
- **Safety:** per-team RBAC from the identity provider instead of a static allowlist, audit
  logs for every action, and action proposals that go through change management.
- **Cost/latency:** route easy cases (high-confidence baseline matches) to a cheap path,
  cache tool results per run, and stream partial findings to Slack.
- **Multi-tenancy:** per-team Slack routing, ownership metadata from the DAG, quieter
  channels.

**"Why CRAG instead of plain RAG?"** Plain top-k always returns something, so the agent
cites irrelevant runbooks with confidence. Grading plus one corrective rewrite gives it a
relevant runbook or an honest "none matched". The grader is deterministic so it's free and
reproducible; at scale you might use a small cross-encoder.

**"Why Socket Mode?"** No public URL, no TLS, no inbound firewall rule; it works on a
laptop. The trade-off is that the app can't go in the Slack Marketplace, which doesn't
matter for an internal tool.

**"What was the hardest bug?"** Some good candidates:
- **Masked passwords:** Airflow masks every occurrence of a connection password in logs.
  With the password `warehouse`, the word "warehouse" became `***` everywhere and destroyed
  evidence.
- **Disk-full that never failed:** the report job pruned old files *before* writing, freeing
  exactly enough space for the write to succeed.
- **Gemini `thought_signature`:** Gemini 3 rejects a follow-up turn unless the tool call's
  `thought_signature` is echoed back.
