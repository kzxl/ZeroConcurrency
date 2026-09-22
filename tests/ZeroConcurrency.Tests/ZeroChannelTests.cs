using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ZeroPlatform.Concurrency.Tests
{
    public class ZeroChannelTests
    {
        [Fact]
        public async Task Channel_WriteAndReadAsync_TransfersDataCleanly()
        {
            var channel = new ZeroChannel<string>(64);

            var producer = Task.Run(async () =>
            {
                await channel.WriteAsync("alpha");
                await channel.WriteAsync("beta");
                await channel.WriteAsync("gamma");
                channel.Complete();
            });

            var received = new List<string>();
            await foreach (var item in channel.ReadAllAsync())
            {
                received.Add(item);
            }

            await producer;

            Assert.Equal(new[] { "alpha", "beta", "gamma" }, received);
            Assert.True(channel.IsCompleted);
        }

        [Fact]
        public void Channel_TryWriteAndTryRead_SynchronousFastPath()
        {
            var channel = new ZeroChannel<int>(16);

            Assert.True(channel.TryWrite(1));
            Assert.True(channel.TryWrite(2));
            Assert.Equal(2, channel.Count);

            Assert.True(channel.TryRead(out int val1));
            Assert.Equal(1, val1);
            Assert.True(channel.TryRead(out int val2));
            Assert.Equal(2, val2);
            Assert.Equal(0, channel.Count);
        }

        [Fact]
        public async Task Channel_WriteToClosed_ThrowsClosedException()
        {
            var channel = new ZeroChannel<int>(16);
            channel.Complete();

            Assert.Throws<ZeroChannelClosedException>(() =>
            {
                channel.TryWrite(99);
            });

            await Assert.ThrowsAsync<ZeroChannelClosedException>(async () =>
            {
                await channel.WriteAsync(99);
            });
        }
    }
}
