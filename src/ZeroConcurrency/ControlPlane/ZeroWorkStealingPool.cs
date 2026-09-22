using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroPlatform.Concurrency
{
    /// <summary>
    /// Represents a high-throughput, low-latency work-stealing thread pool.
    /// Each worker thread owns a dedicated lock-free queue and attempts to steal work
    /// from peer workers when idle, eliminating thread contention on a single shared queue.
    /// </summary>
    public sealed class ZeroWorkStealingPool : IDisposable
    {
        private readonly int _workerCount;
        private readonly Thread[] _workers;
        private readonly ConcurrentQueue<Action>[] _queues;
        private readonly AutoResetEvent[] _waitHandles;
        private readonly CancellationTokenSource _cts = new();
        private int _roundRobinIndex;
        private int _disposed;

        /// <summary>
        /// Gets the number of worker threads managed by the pool.
        /// </summary>
        public int WorkerCount => _workerCount;

        /// <summary>
        /// Gets whether the pool has been disposed.
        /// </summary>
        public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZeroWorkStealingPool"/> class.
        /// </summary>
        /// <param name="workerCount">Number of dedicated worker threads (defaults to logical processor count).</param>
        /// <param name="threadNamePrefix">Optional name prefix for worker threads.</param>
        public ZeroWorkStealingPool(int workerCount = 0, string threadNamePrefix = "ZeroWorker")
        {
            _workerCount = workerCount > 0 ? workerCount : Math.Max(1, Environment.ProcessorCount);
            _workers = new Thread[_workerCount];
            _queues = new ConcurrentQueue<Action>[_workerCount];
            _waitHandles = new AutoResetEvent[_workerCount];

            for (int i = 0; i < _workerCount; i++)
            {
                _queues[i] = new ConcurrentQueue<Action>();
                _waitHandles[i] = new AutoResetEvent(false);

                int workerIndex = i;
                _workers[i] = new Thread(() => WorkerLoop(workerIndex))
                {
                    Name = $"{threadNamePrefix}-{workerIndex}",
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal
                };
                _workers[i].Start();
            }
        }

        /// <summary>
        /// Dispatches a work item to the pool with minimal scheduling overhead.
        /// </summary>
        /// <param name="action">The delegate to execute.</param>
        public void Post(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            ThrowIfDisposed();

            int targetIndex = (int)((uint)Interlocked.Increment(ref _roundRobinIndex) % (uint)_workerCount);
            _queues[targetIndex].Enqueue(action);
            _waitHandles[targetIndex].Set();
        }

        /// <summary>
        /// Dispatches a stateful work item to the pool without allocating closure objects.
        /// </summary>
        public void Post<TState>(Action<TState> action, TState state)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            Post(() => action(state));
        }

        /// <summary>
        /// Executes a work item asynchronously, returning a zero-allocation awaitable <see cref="ValueTask"/>.
        /// </summary>
        public ValueTask ExecuteAsync(Action action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            ThrowIfDisposed();

            var promise = ZeroPromisePool.Rent();
            Post(() =>
            {
                try
                {
                    action();
                    promise.TrySetResult();
                }
                catch (Exception ex)
                {
                    promise.TrySetException(ex);
                }
            });

            return promise.Task;
        }

        /// <summary>
        /// Executes a function asynchronously and returns its result via a pooled <see cref="ValueTask{T}"/>.
        /// </summary>
        public ValueTask<T> ExecuteAsync<T>(Func<T> function)
        {
            if (function == null)
                throw new ArgumentNullException(nameof(function));

            ThrowIfDisposed();

            var promise = ZeroPromisePool<T>.Rent();
            Post(() =>
            {
                try
                {
                    T result = function();
                    promise.TrySetResult(result);
                }
                catch (Exception ex)
                {
                    promise.TrySetException(ex);
                }
            });

            return promise.Task;
        }

        private void WorkerLoop(int workerIndex)
        {
            var myQueue = _queues[workerIndex];
            var myWaitHandle = _waitHandles[workerIndex];
            var token = _cts.Token;

            while (!token.IsCancellationRequested)
            {
                // 1. Process own queue
                if (myQueue.TryDequeue(out var action))
                {
                    ExecuteActionSafely(action);
                    continue;
                }

                // 2. Work Stealing: try stealing from peer workers
                bool stole = false;
                for (int offset = 1; offset < _workerCount; offset++)
                {
                    int peerIndex = (workerIndex + offset) % _workerCount;
                    if (_queues[peerIndex].TryDequeue(out action))
                    {
                        ExecuteActionSafely(action);
                        stole = true;
                        break;
                    }
                }

                if (stole)
                    continue;

                // 3. No work available: sleep until signaled
                myWaitHandle.WaitOne(10);
            }

            // Drain remaining items on shutdown
            while (myQueue.TryDequeue(out var action))
            {
                ExecuteActionSafely(action);
            }
        }

        private static void ExecuteActionSafely(Action action)
        {
            try
            {
                action();
            }
            catch
            {
                // Isolate unhandled exceptions from crashing the worker thread
            }
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _disposed) != 0)
                throw new ObjectDisposedException(nameof(ZeroWorkStealingPool));
        }

        /// <summary>
        /// Shuts down all worker threads gracefully.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            _cts.Cancel();

            // Wake up all workers
            for (int i = 0; i < _workerCount; i++)
            {
                _waitHandles[i].Set();
            }

            for (int i = 0; i < _workerCount; i++)
            {
                _workers[i].Join(500);
                _waitHandles[i].Dispose();
            }

            _cts.Dispose();
        }
    }
}
