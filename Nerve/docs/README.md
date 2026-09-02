# Nerve documentation

Built by Gravicode Studios, led by Kang Fadhil.

## Pages

| | |
|---|---|
| [getting-started.md](getting-started.md) | Install, publish, subscribe, and the two mistakes worth avoiding |
| [patterns.md](patterns.md) | Wildcards, retained messages, request/reply, streams, waiting |
| [architecture.md](architecture.md) | How a publish becomes a handler call, and why it allocates nothing |
| [api-reference.md](api-reference.md) | Every public member |
| [performance.md](performance.md) | What was measured, on what, and what it means |
| [agent-simulator.md](agent-simulator.md) | How the Avalonia panel is put together |
| [migration-v2.md](migration-v2.md) | What changed from v1 and what to do about it |

Bahasa Indonesia: [id/](id/).

## Screenshots

![The agent coordination simulator with five specialists working](images/agent-sim-flow.png)

![A finished mission, aggregated from three specialists](images/agent-sim.png)

## Raw data

- [benchmark-run.txt](benchmark-run.txt) — the sustained-load output quoted in
  [performance.md](performance.md)
- [benchmark-micro.txt](benchmark-micro.txt) — the BenchmarkDotNet output for the same run
- [legacy-readme.md](legacy-readme.md) — the v1 README, kept for reference

Every number in [performance.md](performance.md) came from those two files. If you change something
on the publish path, re-run the harness and update both, or say plainly that the figures predate the
change.
