"""beacon-mcp: the read-only investigation tools from beacon/tools.py as an MCP server.

Transports: stdio (local clients such as Claude Code / Claude Desktop) and streamable
HTTP (inside docker compose, served at http://<host>:8200/mcp).
"""
from __future__ import annotations

import argparse
import json
import logging

from mcp.server.fastmcp import FastMCP

from beacon import tools
from beacon.config import get_settings
from beacon.retrieval.runbooks import Runbook, load_runbooks

INSTRUCTIONS = ("Beacon investigation tools for an Apache Airflow data platform. All tools are read-only. "
                "Start with get_platform_status or list_recent_failures, read the failing task's log with "
                "get_task_logs, and use search_runbooks to find the fix. Runbooks are also resources "
                "(runbook://<id>).")


async def _call(name: str, args: dict) -> str:
    result = await tools.call_tool(tools.READ_ONLY_TOOLS[name], args)
    return json.dumps(result, indent=1, default=str)


def build_server(host: str = "127.0.0.1", port: int = 8200) -> FastMCP:
    mcp = FastMCP("beacon", instructions=INSTRUCTIONS, host=host, port=port)

    @mcp.tool()
    async def get_platform_status() -> str:
        """Scheduler health, per-DAG success rate over the last 24h, and open incidents."""
        return await _call("get_platform_status", {})

    @mcp.tool()
    async def list_recent_failures(dag_id: str | None = None, since_minutes: int = 60) -> str:
        """Recent failed DAG runs and their failed tasks. dag_id is optional; since_minutes is 1-1440."""
        return await _call("list_recent_failures", {"dag_id": dag_id, "since_minutes": since_minutes})

    @mcp.tool()
    async def get_dag_run(dag_id: str, run_id: str) -> str:
        """Details of one DAG run and the state of each of its tasks."""
        return await _call("get_dag_run", {"dag_id": dag_id, "run_id": run_id})

    @mcp.tool()
    async def get_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int | None = None,
                            tail_lines: int = 40) -> str:
        """The last lines of a task's log (latest try by default). tail_lines is 5-150."""
        return await _call("get_task_logs", {"dag_id": dag_id, "run_id": run_id, "task_id": task_id,
                                             "try_number": try_number, "tail_lines": tail_lines})

    @mcp.tool()
    async def query_prometheus(promql: str, minutes: int = 0) -> str:
        """Run a read-only PromQL query. minutes=0 is an instant query; otherwise a range over the last N
        minutes (capped). Useful metrics: airflow_ti_finish, airflow_task_duration_bucket,
        airflow_dagrun_duration_count, airflow_scheduler_heartbeat, airflow_sla_missed."""
        return await _call("query_prometheus", {"promql": promql, "minutes": minutes})

    @mcp.tool()
    async def search_runbooks(query: str, k: int = 3) -> str:
        """Find runbooks relevant to a symptom or error message, with relevance scores."""
        return await _call("search_runbooks", {"query": query, "k": k})

    @mcp.tool()
    async def get_runbook(runbook_id: str) -> str:
        """Full text of a runbook by id (e.g. rb-disk-full)."""
        return await _call("get_runbook", {"runbook_id": runbook_id})

    for rb in load_runbooks(get_settings().runbooks_dir):
        _register_runbook(mcp, rb)
    return mcp


def _register_runbook(mcp: FastMCP, rb: Runbook) -> None:
    def read() -> str:
        return f"# {rb.title}\n\nRoot cause: {rb.root_cause}\n\n" + rb.body

    mcp.resource(f"runbook://{rb.id}", name=rb.id, description=rb.title, mime_type="text/markdown")(read)


def main() -> None:
    parser = argparse.ArgumentParser(prog="beacon-mcp", description=__doc__)
    parser.add_argument("--transport", choices=["stdio", "http"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8200)
    args = parser.parse_args()
    # stdout carries the stdio protocol, so logs go to stderr.
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    server = build_server(args.host, args.port)
    server.run(transport="stdio" if args.transport == "stdio" else "streamable-http")


if __name__ == "__main__":
    main()
