using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using ZeroPlatform.Concurrency;

namespace ZeroConcurrency.Tests
{
    public class AdvancedConcurrencyTests
    {
        [Fact]
        public async Task AsyncManualResetEvent_Basic_SetAndReset_Succeeds()
        {
            var mre = new AsyncManualResetEvent(initialState: false);
            Assert.False(mre.IsSet);

            var waitTask = mre.WaitAsync();
            Assert.False(waitTask.IsCompleted);

            mre.Set();
            Assert.True(mre.IsSet);
            await waitTask;

            // Subsequent waits should complete synchronously (0 allocation)
            var immediateWait = mre.WaitAsync();
            Assert.True(immediateWait.IsCompleted);

            mre.Reset();
            Assert.False(mre.IsSet);

            var newWait = mre.WaitAsync();
            Assert.False(newWait.IsCompleted);

            mre.Set();
            await newWait;
        }

        [Fact]
        public async Task AsyncAutoResetEvent_UnblocksOneWaiterAtATime()
        {
            var are = new AsyncAutoResetEvent(initialState: false);
            Assert.False(are.IsSet);

            var waitTask1 = are.WaitAsync();
            var waitTask2 = are.WaitAsync();

            Assert.False(waitTask1.IsCompleted);
            Assert.False(waitTask2.IsCompleted);

            // Signal first waiter
            are.Set();
            await waitTask1;
            Assert.False(are.IsSet); // Should auto-reset
            Assert.False(waitTask2.IsCompleted);

            // Signal second waiter
            are.Set();
            await waitTask2;
            Assert.False(are.IsSet);
        }

        [Fact]
        public async Task ZeroWorkStealingPool_ExecuteAsync_ParallelJobs_Succeeds()
        {
            using var pool = new ZeroWorkStealingPool(workerCount: 4);
            Assert.Equal(4, pool.WorkerCount);
            Assert.False(pool.IsDisposed);

            int counter = 0;
            const int totalJobs = 100;
            var tasks = new Task[totalJobs];

            for (int i = 0; i < totalJobs; i++)
            {
                tasks[i] = pool.ExecuteAsync(() =>
                {
                    Interlocked.Increment(ref counter);
                }).AsTask();
            }

            await Task.WhenAll(tasks);
            Assert.Equal(totalJobs, counter);

            // Test function with return value
            var result = await pool.ExecuteAsync(() => 42 * 2);
            Assert.Equal(84, result);
        }

        [Fact]
        public void ZeroNativeRingBuffer_WriteAndRead_UnmanagedMemory_Succeeds()
        {
            using var ring = new ZeroNativeRingBuffer(capacityPowerOfTwo: 16);
            Assert.Equal(16, ring.Capacity);
            Assert.Equal(0, ring.Count);

            byte[] sampleData = new byte[] { 10, 20, 30, 40, 50 };
            bool writeSuccess = ring.TryWrite(sampleData);
            Assert.True(writeSuccess);
            Assert.Equal(1, ring.Count);

            Span<byte> readDest = stackalloc byte[16];
            bool readSuccess = ring.TryRead(readDest, out int bytesRead);

            Assert.True(readSuccess);
            Assert.Equal(sampleData.Length, bytesRead);
            for (int i = 0; i < sampleData.Length; i++)
            {
                Assert.Equal(sampleData[i], readDest[i]);
            }
            Assert.Equal(0, ring.Count);
        }

        [Fact]
        public void ZeroNativeRingBuffer_FullCapacity_RejectsOverwrites()
        {
            using var ring = new ZeroNativeRingBuffer(capacityPowerOfTwo: 4);

            byte[] data = new byte[] { 1, 2, 3 };

            // Fill 4 slots
            for (int i = 0; i < 4; i++)
            {
                Assert.True(ring.TryWrite(data));
            }

            Assert.Equal(4, ring.Count);

            // 5th write must be rejected
            Assert.False(ring.TryWrite(data));

            // Read 1 item
            Span<byte> dest = stackalloc byte[4];
            Assert.True(ring.TryRead(dest, out _));
            Assert.Equal(3, ring.Count);

            // Now write should succeed
            Assert.True(ring.TryWrite(data));
            Assert.Equal(4, ring.Count);
        }
    }
}
