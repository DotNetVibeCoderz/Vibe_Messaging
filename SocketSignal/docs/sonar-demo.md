# The sonar console

*Gravicode Studios, led by Kang Fadhil.*

![The sonar console](images/sonar-console.png)

A sea sonar simulator, built with Avalonia, that exists to put SocketSignal under a real load
rather than illustrate it with a diagram.

```bash
dotnet run --project src/SocketSignal.SonarDemo
```

## What is actually happening

Two SocketSignal peers run in the same process, and they talk over a real WebSocket:

**The array** (`SonarStation`) is a `SocketSignalServer` on `ws://localhost:8123/sonar/`. It owns
the sea: seven contacts, each with a bearing, range, course and speed, advanced twenty times a
second. After each step it pushes a `SweepFrame` — the beam bearing plus every echo — to the
`operators` group.

**The console** (`MainWindow`) is a `SocketSignalClient`. It calls `sonar.attach` to be put in the
operators group, then draws whatever arrives. It holds no sea state of its own; if the link stops,
the picture stops.

That constraint is the point. The console could read `SonarStation` directly — same process, same
memory — and the demo would prove nothing. Going over the socket means the code here is the code
that would run with the consoles two decks up.

### Every feature, exercised

| Feature | Where |
|---|---|
| Client → server, with a return value | `sonar.attach`, `sonar.classify`, `sonar.ping` |
| Server → group | the sweep frame, 20×/second to `operators` |
| Groups | the console joins `operators` inside the `sonar.attach` handler |
| Typed single-argument calls | `SendToGroupAsync(..., frame)` — one record, no `object[]` |
| Error propagation | classify a track the array has lost, and the message comes back |
| Keepalive + auto-reconnect | kill the array and the link lamp goes red, then recovers |

Pressing **Classify** is the clearest one: it calls `sonar.classify` and waits about half a second
for the array to answer. That delay is deliberate — it is what makes it a request-response call
rather than a broadcast, and it is why the button reads *Studying return* while it waits.

![Selecting a contact and classifying it](images/sonar-classify.png)

## The two instruments

The console shows the same contacts twice, which is the thing that makes it a sonar console rather
than a radar screen with dots on it.

**The plan position indicator** answers *where is it*. The beam sweeps clockwise from north at
60°/second; a contact is at full brightness the moment the beam passes it and fades as the beam
moves on, exactly as a phosphor tube behaves. So brightness is not decoration — it tells the
operator how long ago the array actually heard something. Range rings are drawn every 3 km, and
the dashed red ring at 2.5 km is close quarters: a contact inside it turns red, the only saturated
red on the console.

**The bearing-time recorder** answers *what has it been doing*. Bearing runs across, time runs
down, newest at the top, and each contact draws a trace over the last 120 seconds. This is the
instrument a real sonar operator reads, and it shows what the scope cannot: a trace that runs
straight down is a contact holding its bearing, which is the classic signature of something on a
collision course. A trace drawing left or right is a contact crossing.

The two are tied together by a hairline on the recorder marking where the beam is, and by
selection — clicking a contact in either the list or the scope highlights it in both.

## The visual design

The brief was "sonar", and the default answer to that brief is a black screen with an acid-green
sweep. That is the film version, not the instrument, so the console is grounded somewhere else:

**Colour.** The ground is deep blue-green (`#08131A`) — wet chart paper under night lighting,
never black. Returns are a pale aquamarine (`#7FD4C1`): phosphor-adjacent, but sea-derived rather
than the `#39FF14` of every sonar screensaver. Classification uses chart conventions —
amber for surface, pale blue for submerged, and Admiralty magenta for unidentified, magenta being
the colour real charts reserve for caution. Red appears exactly once, at close quarters, so that
when it appears it means something.

**Type.** Panel labels are set small, uppercase and widely tracked, the way instrument silkscreen
is lettered. Every number is monospaced, because a bearing that changes width as it changes value
is unreadable at a glance — that is a functional requirement on this screen, not a stylistic one.

**Structure.** The structural devices encode information: bearing in degrees, ranges in kilometres,
time in seconds behind now. There is no decorative numbering, because nothing here is a sequence.

The whole palette and type scale live in
[`Theme.axaml`](../src/SocketSignal.SonarDemo/Theme.axaml), one file, commented.

## Code layout

```
src/SocketSignal.SonarDemo/
├── Simulation/
│   ├── Contact.cs        the wire records: ContactEcho, SweepFrame, ClassificationResult
│   ├── SonarStation.cs   the array: the server, the sea, and the methods it exposes
│   └── ConsoleModel.cs   what the console knows, assembled from the frames it receives
├── Controls/
│   ├── PpiScope.cs       the plan position indicator
│   ├── BearingWaterfall.cs the bearing-time recorder
│   └── Sparkline.cs      the telemetry trace
├── Theme.axaml           palette and type
└── MainWindow.axaml(.cs) layout, and the network wiring
```

Both instruments are drawn in `Render(DrawingContext)` rather than composed from XAML shapes: at
60 fps with trails and 120 seconds of history, retained shapes would be the wrong tool.

## Things worth trying

- **Watch the telemetry strip.** It reports frames pushed, kilobytes, and bytes per frame straight
  from `SignalStatistics`. A seven-contact sweep frame is about 1.2 KB, twenty times a second.
- **Press Hold.** The console stops applying frames; the array keeps sending. Release and it
  catches up from the live picture, not from a queue — there is no backlog to replay.
- **Press Active ping.** One call, and every contact in range answers at full strength for three
  seconds. The reply is the number illuminated.
- **Classify the biologic.** The array reports *broadband, no tonals* — the low-speed,
  strong-return case in `SonarStation.Classify`.
- **Kill the array.** Stop the process' server half and the link lamp turns red; the client's
  `AutoReconnect` starts backing off and the status rail counts the attempts.
