using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace ZeroPlatform.Concurrency
{
    /// <summary>
    /// Reusable <see cref="IValueTaskSource{T}"/> implementation that eliminates GC allocations for asynchronous completions.
    /// Automatically resets and returns to <see cref="ZeroPromisePool{T}"/> immediately after the awaiter consumes the result.
    /// </summary>
    /// <typeparam name="T">The result type.</typeparam>
    public sealed class ZeroPromise<T> : IValueTaskSource<T>, IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<T> _core;
        private readonly Action<ZeroPromise<T>>? _returnToPool;

        internal ZeroPromise(Action<ZeroPromise<T>>? returnToPool = null)
        {
            _returnToPool = returnToPool;
            _core.RunContinuationsAsynchronously = true; // Prevent stack overflows on long continuation chains
        }

        /// <summary>
        /// Gets a <see cref="ValueTask{T}"/> wrapping this promise.
        /// </summary>
        public ValueTask<T> Task => new ValueTask<T>(this, _core.Version);

        /// <summary>
        /// Transitions the promise to a completed state with the specified result.
        /// </summary>
        /// <param name="result">The result value.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetResult(T result)
        {
            _core.SetResult(result);
        }

        /// <summary>
        /// Transitions the promise to a faulted state with the specified exception.
        /// </summary>
        /// <param name="exception">The exception.</param>
        public void SetException(Exception exception)
        {
            _core.SetException(exception);
        }

        /// <summary>
        /// Gets the current status of the operation.
        /// </summary>
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        /// <summary>
        /// Schedules the continuation action that will be invoked when the operation completes.
        /// </summary>
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _core.OnCompleted(continuation, state, token, flags);
        }

        /// <summary>
        /// Retrieves the result and recycles the instance back to the object pool.
        /// </summary>
        public T GetResult(short token)
        {
            try
            {
                return _core.GetResult(token);
            }
            finally
            {
                _core.Reset();
                _returnToPool?.Invoke(this);
            }
        }

        void IValueTaskSource.GetResult(short token) => GetResult(token);
    }

    /// <summary>
    /// Reusable <see cref="IValueTaskSource"/> implementation for non-generic (void) asynchronous operations.
    /// </summary>
    public sealed class ZeroPromise : IValueTaskSource
    {
        private ManualResetValueTaskSourceCore<bool> _core;
        private readonly Action<ZeroPromise>? _returnToPool;

        internal ZeroPromise(Action<ZeroPromise>? returnToPool = null)
        {
            _returnToPool = returnToPool;
            _core.RunContinuationsAsynchronously = true;
        }

        /// <summary>
        /// Gets a <see cref="ValueTask"/> wrapping this promise.
        /// </summary>
        public ValueTask Task => new ValueTask(this, _core.Version);

        /// <summary>
        /// Transitions the promise to a completed state.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetResult() => _core.SetResult(true);

        /// <summary>
        /// Transitions the promise to a faulted state with an exception.
        /// </summary>
        public void SetException(Exception exception) => _core.SetException(exception);

        /// <summary>
        /// Gets the current status of the operation.
        /// </summary>
        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        /// <summary>
        /// Schedules the continuation action.
        /// </summary>
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            _core.OnCompleted(continuation, state, token, flags);
        }

        /// <summary>
        /// Retrieves the result and recycles the instance back to the object pool.
        /// </summary>
        public void GetResult(short token)
        {
            try
            {
                _core.GetResult(token);
            }
            finally
            {
                _core.Reset();
                _returnToPool?.Invoke(this);
            }
        }
    }

    /// <summary>
    /// Lock-free pool managing recyclable <see cref="ZeroPromise{T}"/> instances to eliminate GC allocations.
    /// Equipped with a single-slot fast path (sub-3ns latency) and a concurrent queue fallback.
    /// </summary>
    public static class ZeroPromisePool<T>
    {
        private static ZeroPromise<T>? s_fastItem;
        private static readonly ConcurrentQueue<ZeroPromise<T>> s_pool = new ConcurrentQueue<ZeroPromise<T>>();

        /// <summary>
        /// Rents a reusable promise from the pool.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ZeroPromise<T> Rent()
        {
            // Fast path: atomically claim the cached item without queue contention
            var fast = Interlocked.Exchange(ref s_fastItem, null);
            if (fast != null)
            {
                return fast;
            }

            if (s_pool.TryDequeue(out var promise))
            {
                return promise;
            }

            return new ZeroPromise<T>(Return);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Return(ZeroPromise<T> promise)
        {
            // Fast path: store into fast slot if available; otherwise spill over to queue
            if (Interlocked.CompareExchange(ref s_fastItem, promise, null) != null)
            {
                s_pool.Enqueue(promise);
            }
        }
    }

    /// <summary>
    /// Lock-free pool managing recyclable non-generic <see cref="ZeroPromise"/> instances.
    /// </summary>
    public static class ZeroPromisePool
    {
        private static ZeroPromise? s_fastItem;
        private static readonly ConcurrentQueue<ZeroPromise> s_pool = new ConcurrentQueue<ZeroPromise>();

        /// <summary>
        /// Rents a reusable non-generic promise from the pool.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ZeroPromise Rent()
        {
            var fast = Interlocked.Exchange(ref s_fastItem, null);
            if (fast != null)
            {
                return fast;
            }

            if (s_pool.TryDequeue(out var promise))
            {
                return promise;
            }

            return new ZeroPromise(Return);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void Return(ZeroPromise promise)
        {
            if (Interlocked.CompareExchange(ref s_fastItem, promise, null) != null)
            {
                s_pool.Enqueue(promise);
            }
        }
    }
}
