using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace Npgsql;

sealed class SingleThreadSynchronizationContext : SynchronizationContext, IDisposable
{
    readonly BlockingCollection<CallbackAndState> _tasks = new();
    Thread? _thread;

    const int ThreadStayAliveMs = 10000;
    readonly string _threadName;

    internal SingleThreadSynchronizationContext(string threadName)
        => _threadName = threadName;

    internal Disposable Enter() => new(this);

    public override void Post(SendOrPostCallback callback, object? state)
    {
        _tasks.Add(new CallbackAndState { Callback = callback, State = state });

        if (_thread == null)
        {
            lock (this)
            {
                if (_thread != null)
                    return;
                _thread = new Thread(WorkLoop) { Name = _threadName, IsBackground = true };
                _thread.Start();
            }
        }
    }

    public void Dispose()
    {
        _tasks.CompleteAdding();
        _tasks.Dispose();

        lock (this)
        {
            _thread?.Join();
        }
    }

    void WorkLoop()
    {
        SetSynchronizationContext(this);

        try
        {
            while (true)
            {
                var taken = _tasks.TryTake(out var callbackAndState, ThreadStayAliveMs);
                if (!taken)
                    return;
                callbackAndState.Callback(callbackAndState.State);
            }
        }
        catch (Exception e)
        {
            Trace.Write($"Exception caught in {nameof(SingleThreadSynchronizationContext)}:" + Environment.NewLine + e);
        }
        finally
        {
            lock (this) { _thread = null; }
        }
    }

    struct CallbackAndState
    {
        internal SendOrPostCallback Callback;
        internal object? State;
    }

    internal readonly struct Disposable : IDisposable
    {
        readonly SynchronizationContext? _synchronizationContext;

        internal Disposable(SynchronizationContext synchronizationContext)
        {
            _synchronizationContext = Current;
            SetSynchronizationContext(synchronizationContext);
        }

        public void Dispose()
            => SetSynchronizationContext(_synchronizationContext);
    }
}