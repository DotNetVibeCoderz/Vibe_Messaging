# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

BigPipe is a Kafka-protocol-compatible streaming platform. The data plane is written in **Rust** (a single binary, `bigpiped`) and the control plane and ecosystem in **.NET 10**. It lives in the `BigPipe/` folder of the `Vibe_Messaging` monorepo. Sibling folders (BlackHole, Nerve, SocketSignal) are unrelated projects. Keep CI path filters and working directories scoped to `BigPipe/` (CI: `../.github/workflows/bigpipe-ci.yml`). The Go SDK module is `github.com/DotNetVibeCoderz/Vibe_Messaging/BigPipe/sdk/go`, and it is released by pushing a tag `BigPipe/sdk/go/vX.Y.Z`.

- `requirements.md` holds the owner's requirements (Bahasa Indonesia).
- `solution-design.md` is the full target design (Bahasa Indonesia). It is the source of truth for names, ports and config keys, but **much of it is not built yet**. `PLAN.md` maps each design section to done / planned. Look up sections with `grep -n '^#' solution-design.md`.
- `Progress.md` is the development log. Update it when work lands.

## Build and test

```bash
# Rust (workspace root = BigPipe/)
cargo build --release -p bigpiped -p bpctl
cargo test --workspace                       # unit tests
cargo test -p bp-expr mapping                # a single test (crate + name filter)
./target/release/bigpiped --mode dev --data-dir ./data   # Kafka 9092, HTTP 8082, admin 9644, metrics 9645

# .NET (BigPipe.sln)
dotnet build BigPipe.sln -c Release
dotnet test sdk/dotnet/tests/BigPipe.Client.Tests                       # starts its own bigpiped (needs the release build) on 19092/18082/19644/19645
dotnet test sdk/dotnet/tests/BigPipe.Client.Tests --filter "FullyQualifiedName~AnalyticsTests.Scripting"   # single test
dotnet pack sdk/dotnet/src/<Project> -c Release -o artifacts/nuget

# Against a running bigpiped on the default ports
python tests/compat/python_compat.py          # librdkafka + kafka-python compatibility
python tests/integration/test_features.py     # storage modes, migration, groups, share, flows
cd sdk/python && pip install -e .[test] && pytest
cd sdk/typescript && npm install && npm test
cd sdk/go && go test ./...
cd sdk/java && mvn test
dotnet run -c Release --project samples/BigPipe.Gallery -- --run-all   # all 14 gallery cases, headless

# Notebooks are generated, not hand-edited
python tools/notebooks/build_notebooks.py [--local-feed artifacts/nuget] [--out dir]
python tools/notebooks/run_notebooks.py <dotnet-repl.exe> samples/notebooks
```

## Architecture

**Split rule:** anything that touches every record is in Rust. Anything humans touch, or that runs per configuration, is in .NET. The data plane never depends on the control plane.

Rust crates (`crates/`):
- `bp-protocol`: the Kafka codec (non-flexible API versions, plus ApiVersions v3), record batch v2, compression, murmur2.
- `bp-expr`: bpql filters and mappings.
- `bp-storage`: segments, extents, tiered/diskless storage, the object store and its LRU cache.
- `bp-broker`: shards, the diskless agent, the group coordinator, share groups, flows, the Kafka server.
- `bp-gateway`: the axum HTTP data gateway (8082) and admin REST API (9644).
- `bigpiped`: the binary.

Key mechanics:

- **Shards:** one OS thread with a tokio `current_thread` runtime per core. Each partition is owned by exactly one shard and reached via `ShardCmd` over mpsc, and appends are group-committed. fsync runs on a background flusher thread, never on a shard.
- **Storage:** records are stored in **Kafka wire format**. Offsets are assigned by patching batch headers, and fetch returns bytes as-is.
- **Partitions are sequences of extents:** local segment, tiered object or diskless file range. Changing `bigpipe.storage.mode` only changes where new extents go. That is how online migration keeps offsets.
- **Diskless:** the agent batches all diskless partitions into one object per `diskless_linger_ms`. Offsets are assigned only after the PUT succeeds.

.NET:
- `sdk/dotnet/src`:
  - `BigPipe.Client`: a managed Kafka client plus admin/HTTP/share clients.
  - `BigPipe.Streams`.
  - `BigPipe.Analytics`, plus `.Scripting`, `.ML`, `.Torch` and `.Gravicode`.
- `control-plane/src`:
  - `BigPipe.Console`: Blazor Server. Strings live in `Services/L10n.cs` (EN and ID).
  - `BigPipe.SchemaRegistry`: stored in the `__bp_schemas` topic.
  - `BigPipe.AdminApi`: roles plus an audit trail in `__bp_audit`.
- `samples`:
  - `BigPipe.Gallery`: Avalonia. Cases are in `Cases/*.cs`.
  - `BigPipe.Demo`.
  - `notebooks`.

The Python, TypeScript, Go and Java SDKs are thin clients of the **HTTP gateway**, not of the Kafka protocol. Keep their API names aligned with the table in `docs/en/sdks.md`.

## Conventions and constraints

- Documentation is **bilingual**. Every page exists in `docs/en` and `docs/id`, and `README.md` has a counterpart in `README.id.md`. Update both. Screenshots live in `docs/images` and are regenerated with `tools/screenshots/capture.py` and `BigPipe.Gallery -- --screenshot`.
- The credit line "Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil" appears in the docs, the apps (Console About/footer, Gallery rail, `bpctl --help`, the `bigpiped` banner, `/v1/cluster`) and the package descriptions.
- Use the `frontend-design` skill for UI work (Console, Gallery).
- Benchmark numbers in the docs come from `bpctl bench` runs on a laptop. Design-doc figures are targets. Never present targets as measurements.
- Package publish credentials are **outside the repo** (`C:\Users\mifma\Documents\CodeSandbox\PackageCredentials.txt`), and so are cloud storage test credentials (`...\StorageCred.txt`). Never copy them into the repo, logs or config. Pass them via environment variables only when needed.
- The `.NET` notebooks run in a kernel without `System.Linq.AsyncEnumerable`. Use `CollectAsync`/`TakeAsync` from `BigPipe.Analytics`, not `ToListAsync`.
