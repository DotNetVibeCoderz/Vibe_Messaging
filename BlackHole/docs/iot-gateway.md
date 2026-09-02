# The IoT Gateway simulator

*BlackHole Messaging — Gravicode Studios, led by Kang Fadhil.*

An Avalonia desktop panel that runs a real BlackHole gateway and attaches as many simulated sensor
devices as you like. **Nothing in it is mocked**: every device is a `BlackHoleClient` on its own TCP
socket, publishing to its own topic, answering RPC the gateway sends back down the same connection.

![Twelve devices streaming into the gateway](images/gateway-panel.png)

## Run it

```bash
cd BlackHole
dotnet run --project src/BlackHole.IoTGateway              # empty panel
dotnet run --project src/BlackHole.IoTGateway -- --demo 12 # 12 devices, already running
```

`--demo` calls exactly the commands an operator would — the gateway really listens, the devices
really connect. It exists for screenshots and for showing the panel without a tour of the buttons.

The gateway binds **loopback only**. Every device runs in the same process, so there is no reason to
expose the port to the network, and no reason to make Windows ask about the firewall.

## What you are looking at

**The traffic ribbon** is the panel's centrepiece: a multi-channel strip chart drawn the way a paper
recorder draws one. Time runs right to left, the newest sample sits under the pen head at the right
edge, and the graticule has real major and minor divisions. Each device is one pen, and **that pen
colour is the device's identity everywhere else in the window** — its row marker, its sparkline, its
trace. The ribbon is a legend for the whole panel, not decoration on top of it.

**The plant floor** lists every device: its id and area, what it measures, a sparkline of its last
30 seconds, its current reading, an alarm word when it crosses a threshold, and how many readings it
has sent. Clicking a row draws that device's pen bold in the ribbon and points the command buttons
at it.

**The gateway rail** counts what the server sees — devices, topics, commands answered, firmware
uploads, bytes uploaded, alarms.

**Activity** logs connections, commands, uploads and faults as they happen, with a colour marker per
kind.

## What each control exercises

| Control | The library path it runs |
|---|---|
| **Add device** / **Add ten** | A new `BlackHoleClient` per device, over a real socket |
| **Identify** | Server-to-client RPC — the gateway calls the device |
| **Calibrate** | Server-to-client RPC with an argument, changing device state |
| **Pause** | Holds publishing without dropping the connection |
| **Excursion** | Drives the value to its alarm threshold, so you can watch the panel react |
| **Firmware** | Uploads 4 MiB as a BlackHole stream, chunked, with progress |
| **Sample rate** | 1–120 Hz per device, applied live |

![A firmware upload completing while telemetry keeps flowing](images/gateway-streaming.png)

The screenshot above was taken mid-demo: `room-3` uploaded 4 MiB as a stream (`UPLOADS 1`,
`UPLOADED 4.0 MiB`) while all twelve devices kept publishing at 93 messages/sec and the traces never
paused. That is the point of the panel — the patterns coexist on one connection.

## How it is built

```
Simulation/
  SensorKind.cs        Six sensor profiles: range, drift, noise, thresholds
  Reading.cs           One reading, 20 bytes, fixed binary layout
  SimulatedDevice.cs   A real BlackHoleClient that publishes and serves RPC
  GatewayHost.cs       A real BlackHoleServer that receives and commands

Controls/
  TraceBuffer.cs       Lock-free ring: receive loop writes, UI thread reads
  StripChart.cs        The multi-channel ribbon
  Sparkline.cs         The same trace at row scale

ViewModels/           MainViewModel, DeviceViewModel
Views/                MainWindow
Theme.axaml           Colour and type tokens
Styles.axaml          Control surfaces
```

### Readings are 20 bytes, not JSON

```csharp
public readonly record struct Reading(long TimestampMs, double Value, int Sequence)
{
    public const int Size = 20;
}
```

A gateway taking tens of thousands of readings a second cannot afford to serialise an object per
sample. This is the shape BlackHole is built for: a small fixed struct written straight into the
frame. The timestamp is Unix milliseconds so a device and a gateway on different machines agree
without a shared type.

### Two clocks

Devices publish at up to 500 Hz each. The panel repaints 30 times a second. They are decoupled:

1. The gateway's receive loop calls `TraceBuffer.Add` — an interlocked cursor and one array write,
   no lock, no allocation.
2. A 33 ms `DispatcherTimer` refreshes rows, recomputes rates, drains the log, and ticks the charts.

Render cost is then flat whether it is 4 devices at 2 Hz or 40 at 200 Hz. Binding straight to the
receive loop would peg the dispatcher and freeze the window — this is the pattern to copy for any
high-rate UI.

### Traffic flows one way per path

An earlier version subscribed every connection to `plant/#` so the gateway would "see everything".
That made the broker fan every reading back out to all twelve devices — each one then blocked
writing to peers that were themselves blocked writing, and the whole floor stalled during connect.

The fix is a rule worth remembering: **telemetry goes up, commands come down, and no path carries
both.** The gateway reads readings off each connection's own router; devices subscribe only to
`control/all`.

## The design

The panel is modelled on a **multi-channel chart recorder in a steel instrument cabinet**.

- **Colour is information, never decoration.** The six pen colours come from the ink sets those
  recorders shipped with, and a pen identifies a device. The only saturated reds on screen are alarm
  states.
- **Bahnschrift** for labels — Microsoft's DIN 1451 derivative, the lettering standard for German
  machinery and control panels. **Cascadia Mono** for every numeral, so digits hold their column as
  values change.
- **Six pens, deliberately.** Past six traces on one chart nothing is readable; the seventh device
  reuses the first pen and is told apart by its row.
- **The empty state is an invitation**, not an apology: "Start the gateway, then add a sensor."

## Reusing the pattern

`GatewayHost` is a compact model for a real gateway:

- Loopback or a chosen endpoint, not blindly all interfaces
- One `SharedHeaderCache` across connections, since devices share a topic vocabulary
- Interlocked counters the UI reads on a timer — the gateway must not slow down to be watched
- Per-connection `StreamReceiver`, so two devices uploading `firmware.bin` cannot corrupt each other
- Stream limits set, so a buggy device cannot exhaust process memory

---

*Built by Gravicode Studios, led by Kang Fadhil.*
