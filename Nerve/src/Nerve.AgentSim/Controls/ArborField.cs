// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using Nerve.AgentSim.ViewModels;

namespace Nerve.AgentSim.Controls;

/// <summary>
/// The impulses currently travelling the arbor. One impulse is one message the panel actually
/// observed on the hub - nothing here is decorative traffic.
/// </summary>
/// <remarks>
/// Owned and mutated only on the UI thread. The subscriptions that observe the hub run on the
/// agents' threads and drop their messages into a queue; the panel's frame timer drains that queue
/// and calls <see cref="Emit"/> from the one thread that also draws.
/// </remarks>
public sealed class ArborField(IReadOnlyList<AgentViewModel> agents)
{
    private readonly List<Impulse> _impulses = [];

    /// <summary>The specialists, in the order their terminals are laid out.</summary>
    public IReadOnlyList<AgentViewModel> Agents { get; } = agents;

    /// <summary>What is in flight this frame.</summary>
    public IReadOnlyList<Impulse> Impulses => _impulses;

    /// <summary>Puts an impulse on an axon.</summary>
    /// <param name="agentIndex">Which axon, as an index into <see cref="Agents"/>.</param>
    /// <param name="outbound">True for a sub-task leaving the soma, false for an answer returning.</param>
    public void Emit(int agentIndex, bool outbound)
    {
        if (agentIndex < 0 || agentIndex >= Agents.Count) return;

        // A hard cap, so a burst of a thousand missions cannot turn the frame timer into a
        // rendering benchmark. The oldest impulse gives up its slot.
        if (_impulses.Count >= 220) _impulses.RemoveAt(0);

        _impulses.Add(new Impulse
        {
            Agent = agentIndex,
            Outbound = outbound,
            Position = outbound ? 0 : 1,
        });
    }

    /// <summary>Advances every impulse and retires the ones that have arrived.</summary>
    /// <param name="seconds">Time since the last frame.</param>
    public void Advance(double seconds)
    {
        const double Speed = 1.45;   // axon lengths per second

        for (int i = _impulses.Count - 1; i >= 0; i--)
        {
            Impulse impulse = _impulses[i];
            impulse.Position += impulse.Outbound ? Speed * seconds : -Speed * seconds;

            if (impulse.Position is > 1.0 or < 0.0)
            {
                // Arrival is what makes a terminal or the soma flare.
                if (impulse.Outbound) Agents[impulse.Agent].Pulse = 1;
                _impulses.RemoveAt(i);
                continue;
            }

            _impulses[i] = impulse;
        }

        foreach (AgentViewModel agent in Agents)
            agent.Pulse = Math.Max(0, agent.Pulse - (seconds * 2.4));

        SomaPulse = Math.Max(0, SomaPulse - (seconds * 2.4));
    }

    /// <summary>Flares the soma, when an answer arrives back at the orchestrator.</summary>
    public void FlareSoma() => SomaPulse = 1;

    /// <summary>How brightly the soma is lit, from one down to zero.</summary>
    public double SomaPulse { get; private set; }

    /// <summary>Removes everything in flight.</summary>
    public void Clear()
    {
        _impulses.Clear();
        SomaPulse = 0;
        foreach (AgentViewModel agent in Agents) agent.Pulse = 0;
    }

    /// <summary>One message, part way along its axon.</summary>
    public struct Impulse
    {
        /// <summary>Which axon it is travelling.</summary>
        public int Agent;

        /// <summary>Where along it, from zero at the soma to one at the terminal.</summary>
        public double Position;

        /// <summary>True for a sub-task on its way out, false for an answer coming back.</summary>
        public bool Outbound;
    }
}
