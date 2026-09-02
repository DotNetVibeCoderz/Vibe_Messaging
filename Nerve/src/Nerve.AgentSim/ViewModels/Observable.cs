// Nerve - built by Gravicode Studios, led by Kang Fadhil.
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows.Input;

namespace Nerve.AgentSim.ViewModels;

/// <summary>The smallest thing that can be bound to. No framework, no source generator.</summary>
public abstract class Observable : INotifyPropertyChanged
{
    /// <inheritdoc />
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>Assigns a field and raises a change notification if the value actually moved.</summary>
    /// <typeparam name="T">The property type.</typeparam>
    /// <param name="field">The backing field.</param>
    /// <param name="value">The new value.</param>
    /// <param name="name">Filled in by the compiler.</param>
    /// <returns>True when the value changed.</returns>
    protected bool Set<T>(ref T field, T value, [CallerMemberName] string? name = null)
    {
        if (EqualityComparer<T>.Default.Equals(field, value)) return false;
        field = value;
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        return true;
    }

    /// <summary>Raises a change notification for a property with no backing field of its own.</summary>
    /// <param name="name">The property name.</param>
    protected void Raise([CallerMemberName] string? name = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

/// <summary>A command backed by a delegate.</summary>
/// <param name="execute">What the command does.</param>
/// <param name="canExecute">Whether it can run, or null for always.</param>
public sealed class RelayCommand(Action execute, Func<bool>? canExecute = null) : ICommand
{
    /// <inheritdoc />
    public event EventHandler? CanExecuteChanged;

    /// <inheritdoc />
    public bool CanExecute(object? parameter) => canExecute?.Invoke() ?? true;

    /// <inheritdoc />
    public void Execute(object? parameter) => execute();

    /// <summary>Re-evaluates <see cref="CanExecute"/>.</summary>
    public void Refresh() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
}
