using System;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading;

namespace ZeroPlatform.Concurrency
{
    // 64-byte padded base class for Sequence to prevent false sharing
    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class SequencePad0
    {
        protected long _p0, _p1, _p2, _p3, _p4, _p5, _p6;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class SequenceValue : SequencePad0
    {
        protected long _value;
    }

    [EditorBrowsable(EditorBrowsableState.Never)]
    public abstract class SequencePad1 : SequenceValue
    {
        protected long _p7, _p8, _p9, _p10, _p11, _p12, _p13;
    }

    /// <summary>
    /// Cache-line padded atomic 64-bit sequence counter used for lock-free coordination in Disruptor architectures.
    /// Eliminates CPU false sharing across adjacent cores.
    /// </summary>
    public sealed class Sequence : SequencePad1
    {
        public const long InitialCursorValue = -1L;

        public Sequence(long initialValue = InitialCursorValue)
        {
            _value = initialValue;
        }

        public long Value
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Volatile.Read(ref _value);
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set => Volatile.Write(ref _value, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long Get() => Volatile.Read(ref _value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long value) => Volatile.Write(ref _value, value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompareAndSet(long expected, long value) =>
            Interlocked.CompareExchange(ref _value, value, expected) == expected;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long IncrementAndGet() => Interlocked.Increment(ref _value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long AddAndGet(long delta) => Interlocked.Add(ref _value, delta);

        public override string ToString() => Value.ToString();
    }

    /// <summary>
    /// Wait strategy interface for consumer stages waiting on sequences.
    /// </summary>
    public interface IWaitStrategy
    {
        long WaitFor(long sequence, Sequence cursor, Sequence[] dependentSequences, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Yielding wait strategy that spins briefly and yields CPU timeslices. Ideal for low-latency with balanced CPU usage.
    /// </summary>
    public sealed class YieldingWaitStrategy : IWaitStrategy
    {
        private const int SpinTries = 100;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long WaitFor(long sequence, Sequence cursor, Sequence[] dependentSequences, CancellationToken cancellationToken)
        {
            int counter = SpinTries;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                long availableSequence = GetMinimumSequence(cursor, dependentSequences);
                if (availableSequence >= sequence)
                {
                    return availableSequence;
                }

                if (counter > 0)
                {
                    counter--;
                    Thread.SpinWait(1);
                }
                else
                {
                    Thread.Yield();
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static long GetMinimumSequence(Sequence cursor, Sequence[] dependentSequences)
        {
            if (dependentSequences == null || dependentSequences.Length == 0)
            {
                return cursor.Get();
            }

            long min = cursor.Get();
            for (int i = 0; i < dependentSequences.Length; i++)
            {
                long val = dependentSequences[i].Get();
                if (val < min) min = val;
            }
            return min;
        }
    }

    /// <summary>
    /// Busy-spin wait strategy for ultra-low latency scenarios (e.g. dedicated pinning cores).
    /// </summary>
    public sealed class BusySpinWaitStrategy : IWaitStrategy
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long WaitFor(long sequence, Sequence cursor, Sequence[] dependentSequences, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                long availableSequence = cursor.Get();
                if (dependentSequences != null && dependentSequences.Length > 0)
                {
                    for (int i = 0; i < dependentSequences.Length; i++)
                    {
                        long dep = dependentSequences[i].Get();
                        if (dep < availableSequence) availableSequence = dep;
                    }
                }

                if (availableSequence >= sequence)
                {
                    return availableSequence;
                }

                Thread.SpinWait(1);
            }
        }
    }

    /// <summary>
    /// Sequence barrier coordinating consumer dependencies and cursor tracking without locks.
    /// </summary>
    public sealed class SequenceBarrier
    {
        private readonly Sequence _cursor;
        private readonly Sequence[] _dependentSequences;
        private readonly IWaitStrategy _waitStrategy;

        internal SequenceBarrier(Sequence cursor, Sequence[] dependentSequences, IWaitStrategy waitStrategy)
        {
            _cursor = cursor;
            _dependentSequences = dependentSequences ?? Array.Empty<Sequence>();
            _waitStrategy = waitStrategy;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long WaitFor(long sequence, CancellationToken cancellationToken = default)
        {
            return _waitStrategy.WaitFor(sequence, _cursor, _dependentSequences, cancellationToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetCursor() => _cursor.Get();
    }

    /// <summary>
    /// Pure C# LMAX Disruptor-style ring buffer.
    /// Features pre-allocated slots, cache-line padded sequence counters, direct ref-return event access,
    /// and multi-stage sequence barriers for lock-free pipelining with zero GC allocation.
    /// </summary>
    /// <typeparam name="T">The event type stored in the ring buffer.</typeparam>
    public sealed class DisruptorRing<T>
    {
        private readonly T[] _entries;
        private readonly int _mask;
        private readonly int _capacity;
        private readonly Sequence _cursor = new Sequence(-1);
        private readonly Sequence _nextSequence = new Sequence(-1);
        private readonly IWaitStrategy _waitStrategy;

        private Sequence[] _gatingSequences = Array.Empty<Sequence>();
        private readonly object _gatingLock = new object();

        /// <summary>
        /// Gets the total capacity of the ring buffer.
        /// </summary>
        public int Capacity => _capacity;

        /// <summary>
        /// Gets the current published cursor sequence.
        /// </summary>
        public Sequence Cursor => _cursor;

        /// <summary>
        /// Initializes a new instance of <see cref="DisruptorRing{T}"/> with pre-allocated event factory.
        /// </summary>
        /// <param name="capacityPowerOfTwo">Capacity (must be power of two).</param>
        /// <param name="eventFactory">Optional factory to pre-populate elements for zero GC allocation during execution.</param>
        /// <param name="waitStrategy">Wait strategy for consumer barriers (default YieldingWaitStrategy).</param>
        public DisruptorRing(int capacityPowerOfTwo, Func<T>? eventFactory = null, IWaitStrategy? waitStrategy = null)
        {
            if (capacityPowerOfTwo < 2 || (capacityPowerOfTwo & (capacityPowerOfTwo - 1)) != 0)
                throw new ArgumentException($"Capacity ({capacityPowerOfTwo}) must be a power of two.", nameof(capacityPowerOfTwo));

            _capacity = capacityPowerOfTwo;
            _mask = capacityPowerOfTwo - 1;
            _entries = new T[capacityPowerOfTwo];
            _waitStrategy = waitStrategy ?? new YieldingWaitStrategy();

            if (eventFactory != null)
            {
                for (int i = 0; i < _entries.Length; i++)
                {
                    _entries[i] = eventFactory();
                }
            }
        }

        /// <summary>
        /// Direct reference access to the event slot at the given sequence.
        /// Enables zero-copy reading and in-place event mutation.
        /// </summary>
        public ref T this[long sequence]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => ref _entries[sequence & _mask];
        }

        /// <summary>
        /// Claims the next sequence number for writing.
        /// Spins if the ring buffer is full relative to the slowest gating consumer.
        /// </summary>
        public long Next()
        {
            long next = _nextSequence.IncrementAndGet();
            long wrapPoint = next - _capacity;

            Sequence[] gating = _gatingSequences;
            if (gating.Length > 0)
            {
                while (true)
                {
                    long minGating = long.MaxValue;
                    for (int i = 0; i < gating.Length; i++)
                    {
                        long seq = gating[i].Get();
                        if (seq < minGating) minGating = seq;
                    }

                    if (wrapPoint <= minGating)
                    {
                        break;
                    }

                    Thread.Yield();
                }
            }

            return next;
        }

        /// <summary>
        /// Publishes the sequence, signaling consumers that the event at this sequence is ready.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Publish(long sequence)
        {
            _cursor.Set(sequence);
        }

        /// <summary>
        /// Creates a sequence barrier coordinating consumers waiting on the ring cursor and optional dependent stages.
        /// </summary>
        public SequenceBarrier NewBarrier(params Sequence[] dependentSequences)
        {
            return new SequenceBarrier(_cursor, dependentSequences, _waitStrategy);
        }

        /// <summary>
        /// Adds consumer gating sequences to prevent the ring producer from overwriting unconsumed events.
        /// </summary>
        public void AddGatingSequences(params Sequence[] sequences)
        {
            if (sequences == null || sequences.Length == 0) return;

            lock (_gatingLock)
            {
                int oldLen = _gatingSequences.Length;
                Sequence[] newArr = new Sequence[oldLen + sequences.Length];
                Array.Copy(_gatingSequences, newArr, oldLen);
                Array.Copy(sequences, 0, newArr, oldLen, sequences.Length);
                _gatingSequences = newArr;
            }
        }
    }
}
