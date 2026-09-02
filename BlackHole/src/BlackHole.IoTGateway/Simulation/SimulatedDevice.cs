// BlackHole Messaging - built by Gravicode Studios, led by Kang Fadhil.
using System.Text;
using BlackHole.Hosting;
using BlackHole.Protocol;
using BlackHole.Transport;

namespace BlackHole.IoTGateway.Simulation;

/// <summary>What a device is doing right now.</summary>
public enum DeviceState
{
    Offline,
    Connecting,
    Streaming,
    Paused,
    Faulted,
}

/// <summary>
/// One simulated sensor: a real <see cref="BlackHoleClient"/> on a real socket, publishing readings
/// to the gateway on its own topic and answering commands the gateway sends back.
/// </summary>
/// <remarks>
/// Nothing here is mocked. The device dials the gateway over TCP, publishes to
/// <c>plant/{area}/{deviceId}/{measurement}</c>, and registers RPC handlers the gateway can call -
/// so the panel is exercising the same code path a field device would.
/// </remarks>
public sealed class SimulatedDevice : IAsyncDisposable
{
    /// <summary>Topic every device listens on for gateway-wide instructions.</summary>
    public const string ControlTopic = "control/all";

    private readonly Random _random;
    private readonly byte[] _payloadBuffer = new byte[Reading.Size];
    private BlackHoleClient? _client;
    private CancellationTokenSource? _loop;
    private Task? _publishTask;
    private double _value;
    private double _momentum;
    private int _sequence;
    private long _published;

    public SimulatedDevice(string deviceId, string area, SensorKind kind, int seed)
    {
        DeviceId = deviceId;
        Area = area;
        Profile = SensorProfile.For(kind);
        Topic = $"plant/{area}/{deviceId}/{Profile.TopicSegment}";
        _random = new Random(seed);
        _value = Profile.Nominal;
    }

    /// <summary>Stable device name, as it appears on the plant floor and in the topic.</summary>
    public string DeviceId { get; }

    /// <summary>Which part of the plant this device sits in.</summary>
    public string Area { get; }

    /// <summary>What it measures.</summary>
    public SensorProfile Profile { get; }

    /// <summary>The topic it publishes to.</summary>
    public string Topic { get; }

    /// <summary>Current state.</summary>
    public DeviceState State { get; private set; } = DeviceState.Offline;

    /// <summary>Latest reading it produced.</summary>
    public double CurrentValue => _value;

    /// <summary>Readings published since it came online.</summary>
    public long PublishedCount => Interlocked.Read(ref _published);

    /// <summary>Readings per second. Changing it takes effect on the next tick.</summary>
    public int SampleRateHz { get; set; } = 4;

    /// <summary>
    /// Drives the value towards its alarm threshold, so an operator can watch the panel react.
    /// </summary>
    public bool Excursion { get; set; }

    /// <summary>Raised whenever the device state changes, so the panel can repaint one row.</summary>
    public event Action<SimulatedDevice>? Changed;

    /// <summary>Connects to the gateway and starts publishing.</summary>
    public async Task StartAsync(int port, CancellationToken cancellationToken = default)
    {
        if (State is DeviceState.Streaming or DeviceState.Connecting)
            return;

        SetState(DeviceState.Connecting);
        try
        {
            var options = new TransportOptions
            {
                KeepAliveInterval = TimeSpan.FromSeconds(20),
                // Every device sends the same handful of topics, so one warm cache per device is
                // all it takes to stop paying for header decoding entirely.
                HeaderCacheCapacity = 32,
            };

            _client = await BlackHoleClient.ConnectWithRetryAsync("127.0.0.1", port, attempts: 4, options: options,
                cancellationToken: cancellationToken);

            // The gateway can call these on the device, over the same socket, no inbound port needed.
            _client.Handlers
                .RegisterText("device/identify", _ => $"{DeviceId}|{Area}|{Profile.Label}|{Profile.Unit}")
                .RegisterText("device/calibrate", offset =>
                {
                    if (double.TryParse(offset, out double delta))
                    {
                        _value = Math.Clamp(_value + delta, Profile.Minimum, Profile.Maximum);
                        return $"calibrated to {_value:F2} {Profile.Unit}";
                    }
                    return "calibration needs a number";
                })
                .RegisterText("device/reset", _ =>
                {
                    _value = Profile.Nominal;
                    Excursion = false;
                    return $"{DeviceId} reset to nominal";
                });

            // The one topic a device listens on: gateway-wide instructions. Telemetry is strictly
            // outbound, so a device never receives another device's readings.
            _client.PubSub.Received += (topic, payload) =>
            {
                if (topic == ControlTopic && Encoding.UTF8.GetString(payload.Span) == "reset")
                {
                    _value = Profile.Nominal;
                    Excursion = false;
                }
            };
            await _client.PubSub.SubscribeAsync(ControlTopic, cancellationToken);

            _client.Closed += _ =>
            {
                if (State != DeviceState.Offline)
                    SetState(DeviceState.Faulted);
            };

            _loop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _publishTask = Task.Run(() => PublishLoopAsync(_loop.Token), CancellationToken.None);
            SetState(DeviceState.Streaming);
        }
        catch (Exception)
        {
            SetState(DeviceState.Faulted);
            throw;
        }
    }

    /// <summary>Holds publishing without dropping the connection.</summary>
    public void Pause()
    {
        if (State == DeviceState.Streaming)
            SetState(DeviceState.Paused);
    }

    /// <summary>Resumes publishing.</summary>
    public void Resume()
    {
        if (State == DeviceState.Paused)
            SetState(DeviceState.Streaming);
    }

    /// <summary>Disconnects and goes offline.</summary>
    public async Task StopAsync()
    {
        SetState(DeviceState.Offline);

        if (_loop is not null)
        {
            await _loop.CancelAsync();
            if (_publishTask is not null)
            {
                try { await _publishTask.WaitAsync(TimeSpan.FromSeconds(2)); }
                catch (Exception) { }
            }
            _loop.Dispose();
            _loop = null;
        }

        if (_client is not null)
        {
            await _client.DisposeAsync();
            _client = null;
        }
    }

    /// <summary>
    /// Uploads a firmware image to the gateway as a BlackHole stream, so the panel exercises the
    /// streaming path with something an operator would actually do.
    /// </summary>
    public async Task<long> UploadFirmwareAsync(
        int sizeBytes, IProgress<long>? progress = null, CancellationToken cancellationToken = default)
    {
        if (_client is null)
            throw new InvalidOperationException($"{DeviceId} is offline.");

        var image = new byte[sizeBytes];
        _random.NextBytes(image);

        return await _client.OutgoingStreams.SendAsync(
            $"{DeviceId}/firmware",
            image,
            new StreamDescriptor($"{DeviceId}-firmware.bin", image.Length, "application/octet-stream"),
            chunkSize: 16 * 1024,
            progress: progress,
            cancellationToken: cancellationToken);
    }

    private async Task PublishLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                int rate = Math.Clamp(SampleRateHz, 1, 500);
                await Task.Delay(TimeSpan.FromMilliseconds(1000.0 / rate), cancellationToken);

                if (State != DeviceState.Streaming || _client is null)
                    continue;

                Advance();
                var reading = Reading.Now(_value, ++_sequence);
                reading.WriteTo(_payloadBuffer);

                await _client.PubSub.PublishAsync(Topic, _payloadBuffer, cancellationToken);
                Interlocked.Increment(ref _published);
                Changed?.Invoke(this);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception)
        {
            SetState(DeviceState.Faulted);
        }
    }

    /// <summary>
    /// Moves the value one tick. Momentum plus mean reversion gives a trace that wanders like a real
    /// process instead of jittering around a straight line - which is what makes the strip chart
    /// readable at a glance.
    /// </summary>
    private void Advance()
    {
        double target = Excursion ? Profile.AlarmAbove * 1.05 : Profile.Nominal;
        double pull = (target - _value) * (Excursion ? 0.045 : 0.012);
        double kick = (_random.NextDouble() - 0.5) * 2 * Profile.Drift;

        _momentum = (_momentum * 0.82) + kick + pull;
        _value = Math.Clamp(
            _value + _momentum + (_random.NextDouble() - 0.5) * 2 * Profile.Noise,
            Profile.Minimum,
            Profile.Maximum);
    }

    /// <summary>How the current value compares against this sensor's thresholds.</summary>
    public ReadingLevel Level => _value >= Profile.AlarmAbove
        ? ReadingLevel.Alarm
        : _value >= Profile.WarnAbove ? ReadingLevel.Warning : ReadingLevel.Normal;

    private void SetState(DeviceState state)
    {
        State = state;
        Changed?.Invoke(this);
    }

    public async ValueTask DisposeAsync() => await StopAsync();

    /// <summary>Names devices the way a plant does: area prefix plus a number.</summary>
    public static string BuildId(SensorKind kind, int index) => kind switch
    {
        SensorKind.Temperature => $"tank-{index}",
        SensorKind.Pressure => $"line-{index}",
        SensorKind.Humidity => $"room-{index}",
        SensorKind.FlowRate => $"pump-{index}",
        SensorKind.Vibration => $"motor-{index}",
        _ => $"panel-{index}",
    };
}
