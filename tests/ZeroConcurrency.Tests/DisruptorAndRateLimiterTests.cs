using System;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using ZeroPlatform.Concurrency.RateLimiting;

namespace ZeroPlatform.Concurrency.Tests
{
    public class DisruptorAndRateLimiterTests
    {
        [Fact]
        public void TokenBucket_BurstCapacity_AllowsInstantBurst()
        {
            var limiter = new TokenBucketRateLimiter(capacity: 5, tokensPerSecond: 10);

            Assert.Equal(5, limiter.Capacity);
            Assert.Equal(10, limiter.TokensPerSecond);

            for (int i = 0; i < 5; i++)
            {
                Assert.True(limiter.TryAcquire(1.0), $"Burst acquire {i} failed");
            }

            // Exceeded capacity: should fail immediately
            Assert.False(limiter.TryAcquire(1.0));
        }

        [Fact]
        public async Task TokenBucket_ReplenishesOverTime()
        {
            var limiter = new TokenBucketRateLimiter(capacity: 10, tokensPerSecond: 100, initialTokens: 0);

            Assert.False(limiter.TryAcquire(1.0));

            // Wait ~50ms -> should replenish ~5 tokens
            await Task.Delay(50);

            Assert.True(limiter.AvailableTokens >= 1.0);
            Assert.True(limiter.TryAcquire(1.0));
        }

        [Fact]
        public async Task TokenBucket_AcquireAsync_WaitsAndSucceeds()
        {
            var limiter = new TokenBucketRateLimiter(capacity: 5, tokensPerSecond: 50, initialTokens: 0);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            await limiter.AcquireAsync(2.0);
            sw.Stop();

            // 2 tokens at 50/sec should take ~40ms
            Assert.True(sw.ElapsedMilliseconds >= 25, $"AcquireAsync elapsed was too fast: {sw.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task DisruptorRing_SingleProducerSingleConsumer_StreamsDataCorrectly()
        {
            const int count = 5000;
            var ring = new DisruptorRing<int>(1024);
            var barrier = ring.NewBarrier();
            var consumerSeq = new Sequence(-1);
            ring.AddGatingSequences(consumerSeq);

            var consumerTask = Task.Run(() =>
            {
                int received = 0;
                long expectedSeq = 0;

                while (received < count)
                {
                    long available = barrier.WaitFor(expectedSeq);
                    while (expectedSeq <= available && received < count)
                    {
                        int value = ring[expectedSeq];
                        Assert.Equal(received * 2, value);
                        received++;
                        consumerSeq.Set(expectedSeq);
                        expectedSeq++;
                    }
                }
            });

            for (int i = 0; i < count; i++)
            {
                long seq = ring.Next();
                ring[seq] = i * 2;
                ring.Publish(seq);
            }

            await consumerTask;
        }

        [Fact]
        public async Task DisruptorRing_MultiStagePipeline_ExecutesInOrder()
        {
            const int count = 2000;
            var ring = new DisruptorRing<long>(512);

            // Stage 1: doubles the value
            var stage1Barrier = ring.NewBarrier();
            var stage1Seq = new Sequence(-1);

            // Stage 2: depends on Stage 1 completing
            var stage2Barrier = ring.NewBarrier(stage1Seq);
            var stage2Seq = new Sequence(-1);

            // Producer gates on both stages
            ring.AddGatingSequences(stage1Seq, stage2Seq);

            var stage1Task = Task.Run(() =>
            {
                long current = 0;
                while (current < count)
                {
                    long available = stage1Barrier.WaitFor(current);
                    while (current <= available && current < count)
                    {
                        ring[current] *= 2;
                        stage1Seq.Set(current);
                        current++;
                    }
                }
            });

            var stage2Task = Task.Run(() =>
            {
                long current = 0;
                while (current < count)
                {
                    long available = stage2Barrier.WaitFor(current);
                    while (current <= available && current < count)
                    {
                        long value = ring[current];
                        Assert.Equal((current + 1) * 2, value);
                        stage2Seq.Set(current);
                        current++;
                    }
                }
            });

            for (int i = 0; i < count; i++)
            {
                long seq = ring.Next();
                ring[seq] = i + 1;
                ring.Publish(seq);
            }

            await Task.WhenAll(stage1Task, stage2Task);
        }
    }
}
