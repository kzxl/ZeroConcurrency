using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ZeroPlatform.Concurrency
{
    /// <summary>
    /// Exception thrown when attempting to write to or read from a closed <see cref="ZeroChannel{T}"/>.
    /// </summary>
    public sealed class ZeroChannelClosedException : InvalidOperationException
    {
        public ZeroChannelClosedException(string message) : base(message) { }
        public ZeroChannelClosedException(string message, Exception? innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// High-performance CSP (Communicating Sequential Processes) channel inspired by Go channels (chan T).
    /// Uses <see cref="ZeroMpmcRingBuffer{T}"/> as its core ring storage and <see cref="ZeroPromise{T}"/> to eliminate GC allocations when data is readily available.
    /// </summary>
    /// <typeparam name="T">The type of elements flowing through the channel.</typeparam>
    public sealed class ZeroChannel<T>
    {
        private readonly ZeroMpmcRingBuffer<T> _buffer;
        private readonly ConcurrentQueue<ZeroPromise<bool>> _waitingReaders = new ConcurrentQueue<ZeroPromise<bool>>();
        private readonly ConcurrentQueue<ZeroPromise<bool>> _waitingWriters = new ConcurrentQueue<ZeroPromise<bool>>();

        private volatile bool _isCompleted;
        private Exception? _completionError;

        /// <summary>
        /// Initializes a new instance of the <see cref="ZeroChannel{T}"/> class with a power-of-two buffer capacity.
        /// </summary>
        /// <param name="capacityPowerOfTwo">The ring buffer capacity (must be a power of two, e.g. 64, 256, 1024...).</param>
        public ZeroChannel(int capacityPowerOfTwo = 1024)
        {
            _buffer = new ZeroMpmcRingBuffer<T>(capacityPowerOfTwo);
        }

        /// <summary>
        /// Gets the maximum capacity of the channel's underlying ring buffer.
        /// </summary>
        public int Capacity => _buffer.Capacity;

        /// <summary>
        /// Gets the approximate number of items currently buffered in the channel.
        /// </summary>
        public int Count => _buffer.Count;

        /// <summary>
        /// Gets a value indicating whether the channel is marked completed and all buffered items have been consumed.
        /// </summary>
        public bool IsCompleted => _isCompleted && _buffer.IsEmpty;

        /// <summary>
        /// Attempts to write an item into the channel synchronously without blocking.
        /// </summary>
        /// <param name="item">The item to write.</param>
        /// <returns><c>true</c> if the item was enqueued; otherwise <c>false</c>.</returns>
        public bool TryWrite(T item)
        {
            if (_isCompleted)
            {
                throw new ZeroChannelClosedException("Cannot write to a closed channel.", _completionError);
            }

            if (_buffer.TryEnqueue(item))
            {
                // Signal a waiting reader that data is available
                while (_waitingReaders.TryDequeue(out var reader))
                {
                    reader.SetResult(true);
                    break;
                }
                return true;
            }

            return false;
        }

        /// <summary>
        /// Attempts to read an item from the channel synchronously without blocking.
        /// </summary>
        /// <param name="item">The item received.</param>
        /// <returns><c>true</c> if an item was read; otherwise <c>false</c>.</returns>
        public bool TryRead(out T item)
        {
            if (_buffer.TryDequeue(out item))
            {
                // Signal a waiting writer that buffer space has freed up
                while (_waitingWriters.TryDequeue(out var writer))
                {
                    writer.SetResult(true);
                    break;
                }
                return true;
            }

            return false;
        }

        /// <summary>
        /// Asynchronously writes an item to the channel (analogous to 'ch &lt;- val' in Go).
        /// Returns synchronously (0 allocation) when capacity is immediately available.
        /// </summary>
        /// <param name="item">The item to write.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask WriteAsync(T item, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
#if NET8_0_OR_GREATER
                return ValueTask.FromCanceled(cancellationToken);
#else
                return new ValueTask(Task.FromCanceled(cancellationToken));
#endif
            }

            // Fast path: immediate write succeeds
            if (TryWrite(item))
            {
                return default;
            }

            if (_isCompleted)
            {
                throw new ZeroChannelClosedException("Cannot write to a closed channel.", _completionError);
            }

            // Slow path: await an available buffer slot
            return SlowWriteAsync(item, cancellationToken);
        }

        private async ValueTask SlowWriteAsync(T item, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (_isCompleted)
                {
                    throw new ZeroChannelClosedException("Channel was closed while waiting to write.", _completionError);
                }

                if (TryWrite(item))
                {
                    return;
                }

                var promise = ZeroPromisePool<bool>.Rent();
                _waitingWriters.Enqueue(promise);

                // Double check to prevent lost notification race
                if (TryWrite(item))
                {
                    return;
                }

                await promise.Task.ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
        }

        /// <summary>
        /// Asynchronously reads an item from the channel (analogous to 'val := &lt;-ch' in Go).
        /// Returns synchronously (0 allocation) when data is immediately available.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public ValueTask<T> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
            {
#if NET8_0_OR_GREATER
                return ValueTask.FromCanceled<T>(cancellationToken);
#else
                return new ValueTask<T>(Task.FromCanceled<T>(cancellationToken));
#endif
            }

            // Fast path: immediate read from ring buffer
            if (TryRead(out var item))
            {
                return new ValueTask<T>(item);
            }

            if (_isCompleted)
            {
                if (TryRead(out item))
                {
                    return new ValueTask<T>(item);
                }
                throw new ZeroChannelClosedException("Channel is closed and empty.", _completionError);
            }

            // Slow path: register promise and await incoming data
            return SlowReadAsync(cancellationToken);
        }

        private async ValueTask<T> SlowReadAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (TryRead(out var item))
                {
                    return item;
                }

                if (_isCompleted)
                {
                    if (TryRead(out item))
                    {
                        return item;
                    }
                    throw new ZeroChannelClosedException("Channel is closed and empty.", _completionError);
                }

                var promise = ZeroPromisePool<bool>.Rent();
                _waitingReaders.Enqueue(promise);

                // Double check to prevent lost notification race
                if (TryRead(out item))
                {
                    return item;
                }

                await promise.Task.ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return default!;
        }

        /// <summary>
        /// Asynchronously waits until data is available for reading or the channel completes.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns><c>true</c> if data is available; <c>false</c> if the channel is closed and drained.</returns>
        public async ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!_buffer.IsEmpty) return true;
                if (_isCompleted) return !_buffer.IsEmpty;

                var promise = ZeroPromisePool<bool>.Rent();
                _waitingReaders.Enqueue(promise);

                if (!_buffer.IsEmpty) return true;
                if (_isCompleted) return !_buffer.IsEmpty;

                await promise.Task.ConfigureAwait(false);
            }

            return false;
        }

        /// <summary>
        /// Streams all elements from the channel as an <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        public async IAsyncEnumerable<T> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                T item;
                try
                {
                    item = await ReadAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (ZeroChannelClosedException)
                {
                    break;
                }

                yield return item;
            }
        }

        /// <summary>
        /// Marks the channel as complete, preventing further writes. Remaining buffered items can still be read.
        /// </summary>
        /// <param name="error">Optional completion exception.</param>
        public void Complete(Exception? error = null)
        {
            if (_isCompleted) return;

            _completionError = error;
            _isCompleted = true;

            // Wake up waiting writers with closed error
            while (_waitingWriters.TryDequeue(out var writer))
            {
                if (error != null) writer.SetException(error);
                else writer.SetException(new ZeroChannelClosedException("Channel has been closed."));
            }

            // Wake up waiting readers so they can check buffer or finish
            while (_waitingReaders.TryDequeue(out var reader))
            {
                reader.SetResult(true);
            }
        }
    }
}
