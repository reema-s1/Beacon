# Decisions

## Slack in Socket Mode

Socket Mode opens an outbound WebSocket from `beacon-slack` to Slack, so Slack never has to
reach us. That means no public URL, no ngrok, no TLS certificate and no inbound firewall
rule, which matters for a project that has to run with one `docker compose up` on a laptop
(including Windows with Docker Desktop). The trade-off is that Socket Mode apps can't be
listed in the Slack Marketplace and each connection is tied to one app token, which is fine
for an internal tool. Moving to the HTTP Events API later only changes `beacon_slack/app.py`;
the handlers are transport-agnostic.

## The agent is read-only; reruns need a human click and an allowlist

The agent's job is to explain, not to act. It is given only the read-only tools
(`AGENT_TOOLS`); `rerun_task` is in the same module so there is one implementation, but it is
deliberately not registered with the agent or the MCP server, and a test asserts that.
A rerun happens only when a person presses **Rerun task**, confirms in a modal, and is in
`RERUN_ALLOWED_USERS`. The allowlist is checked twice: in the Slack handler (fast feedback)
and again in `POST /actions/rerun` (the API is the real gate, so a crafted request can't
skip it).

This is the same idea as the state-machine gating in UnderCurrent: a model can propose a
transition, but only an authorised actor can make it. LLMs are confidently wrong some of
the time; a wrong explanation costs a minute of someone's attention, a wrong action can
corrupt data or hide the real failure. Rerunning is also usually the wrong first move —
`bad_code`, `missing_input` or `disk_full` fail again until someone fixes the cause — so the
confirmation modal says so.

Other limits in the same spirit: PromQL queries are a single plain expression with capped
range, offset and result size; no tool can run shell commands; secrets come only from the
environment and are masked before anything reaches the LLM or Slack.

## A rule-based baseline

Without a baseline, "the agent gets 85% right" means little. A dozen ordered regexes over the
same log text answer the question "what does the LLM add over grep?". On clean, synthetic
failures the honest answer is "not much on root cause", and the evaluation shows that. What
the agent adds is the rest of the job: it finds the failing run and task itself, reads the
right log, writes a summary a pipeline owner can act on, picks the runbook, and copes with
inputs a regex list doesn't anticipate (and says `unknown` instead of guessing). The baseline
also gives a cheap fallback and a regression check: if the agent ever scores below it, the
prompt or tools broke.

## LocalExecutor

The demo runs five small DAGs. LocalExecutor runs tasks as subprocesses of the scheduler, so
there is no Celery, no Redis, no workers to keep alive — fewer containers, less memory on a
laptop, and fewer moving parts that could fail for reasons unrelated to the scenario under
test. It also makes some scenarios realistic in a useful way: stopping the scheduler really
stops all task execution (`scheduler_down`), and a task's memory blow-up happens inside the
same container. At production scale you'd use the Celery or Kubernetes executor; nothing in
Beacon depends on the executor because it only talks to Airflow's REST API and metrics.

## CRAG-style runbook retrieval

Plain top-k retrieval always returns *something*, and an agent will happily cite an
irrelevant runbook. Corrective RAG adds a check: grade each candidate, and if none is good
enough, rewrite the query from the evidence and try once more; if that also fails, return
nothing and let the agent say so.

* The **grader** blends cosine similarity (MiniLM) with symptom-term overlap. It is
  deterministic rather than an LLM call because free-tier token budgets are tight and the
  evaluation should be reproducible. The threshold (0.45) was calibrated on the runbook
  library: every runbook's symptoms and the real log lines from each scenario clear it,
  and off-topic questions don't.
* The **rewrite** maps error signals (`KeyError`, `503`, `Errno 28`, `SIGKILL`, …) to the
  vocabulary the runbooks use. In practice a bare log line like `KeyError: 'unit_price_usd'`
  scores 0.31 on the first pass and is rescued by the rewrite.
* The library deliberately includes **distractors** (permissions, schema change, DAG import
  error, data quality) so retrieval has to discriminate.

## Smaller choices

* **Free LLM tier by default.** Groq's free tier, through the OpenAI-compatible API, so a
  Gemini or Ollama switch is an `.env` change. Prompts are short, logs are cleaned and cut
  to the relevant tail, tool outputs are capped at 2.5k characters, and the alert context
  includes the run id and failed task so the agent doesn't spend steps rediscovering them.
* **Dedupe on fingerprint + run id.** One incident per failed run, not per alert rule:
  `TaskFailureSpike` for a run already reported by `DagRunFailed` is merged.
* **First-seen counter alerts.** `increase()` can't see the first increment of a new counter
  series, so `DagRunFailed` and `SLAMissed` also fire on series that didn't exist five
  minutes ago (`unless … offset`).
* **One image for all Python services.** api, slack, mcp and the tools container share one
  image with the embedding model baked in, so containers start offline.
* **Simulated OOM kill.** A real cgroup OOM kill in the scheduler container would take the
  scheduler down with the task. The `resource_exhaustion` task runs a watchdog that SIGKILLs
  only its own process at 300 MB, which produces the same evidence Airflow shows for a real
  OOM (`Task exited with return code -9`).
* **Non-dictionary database passwords.** Airflow masks every occurrence of a connection
  password in task logs; with a password of `warehouse`, the word "warehouse" turned into
  `***` everywhere and destroyed evidence.
