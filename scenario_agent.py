#!/usr/bin/env python3
"""
scenario_agent.py — CLI entry point for the Scenario Agent.

Discovers open Power BI Desktop models, lets the user pick one,
then starts an interactive REPL for generating financial scenarios.

For the web UI, run server.py instead.

Usage:
    python scenario_agent.py
"""

import os
import sys
import asyncio

from config import OUTPUT_DIR
from pbi_client import PBIClient, list_pbi_instances
from agent import Agent
from storage.sqlite_storage import SQLiteStorage
from discovery.model_understanding import ModelUnderstanding


async def main():
    print("=" * 55)
    print("  Scenario Agent (CLI)")
    print(f"  Output: {OUTPUT_DIR}")
    print("=" * 55)

    from config import DISCOVERY_API_KEY, SCENARIO_API_KEY
    if not (DISCOVERY_API_KEY or SCENARIO_API_KEY):
        print("ERROR: No API key set. Use ANTHROPIC_API_KEY or DISCOVERY_API_KEY + SCENARIO_API_KEY")
        sys.exit(1)

    # ── Discover open Power BI Desktop models ─────────────────────────────────
    print("\nDiscovering open Power BI Desktop models...")
    instances = await list_pbi_instances()

    conn_str = db_guid = ""

    if not instances:
        print("WARNING: No open Power BI Desktop models found.")
        print("         Make sure Power BI Desktop is open with a model loaded.")

    elif len(instances) == 1:
        inst     = instances[0]
        conn_str = inst["connection_string"]
        db_guid  = inst["database"]
        print(f"Auto-selected: {inst['display_name']} (port {inst['port']})")

    else:
        print("\nOpen models:")
        for i, inst in enumerate(instances):
            print(f"  [{i + 1}] {inst['display_name']}  (port {inst['port']})")
        choice   = input("Select model [1]: ").strip()
        idx      = (int(choice) - 1) if choice.isdigit() else 0
        inst     = instances[max(0, min(idx, len(instances) - 1))]
        conn_str = inst["connection_string"]
        db_guid  = inst["database"]
        print(f"Selected: {inst['display_name']}")

    # ── Connect ───────────────────────────────────────────────────────────────
    pbi = PBIClient()
    if conn_str:
        try:
            await pbi.connect(conn_str, db_guid)
        except Exception as e:
            print(f"WARNING: MCP connection failed: {e}")

    # ── Load Model Understanding ──────────────────────────────────────────────
    storage = SQLiteStorage()
    source_id = pbi.source_id()
    data = storage.load_model_understanding(source_id)
    if not data:
        print("\nERROR: No model understanding found for this data source.")
        print("       Use the web UI (python server.py) to run the Discovery Agent first.")
        await pbi.disconnect()
        sys.exit(1)

    clean = {k: v for k, v in data.items() if not k.startswith("_")}
    mu = ModelUnderstanding.from_dict(clean)
    print(f"\nLoaded model understanding: {mu.model_name} (status: {mu.status})")

    # ── REPL ──────────────────────────────────────────────────────────────────
    agent = Agent(pbi, mu)
    print("\nExample: 'Create a 2026 scenario: +2% revenue in Feb, +5% COGS in March'")
    print("Commands: reset | exit\n")

    while True:
        try:
            msg = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break
        if not msg:
            continue
        if msg.lower() == "exit":
            break
        if msg.lower() == "reset":
            agent.reset()
            continue
        print(f"\nAgent: {await agent.chat(msg)}\n")

    await pbi.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
