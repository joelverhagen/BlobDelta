using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Knapcode.Delta.Common.Test.Support;
using Xunit;
using Xunit.Abstractions;

namespace Knapcode.Delta.Common.Test.Functional
{
    public class TaskQueueFacts
    {
        private readonly RecordingLogger<TaskQueueFacts> _logger;

        public TaskQueueFacts(ITestOutputHelper output)
        {
            _logger = output.GetLogger<TaskQueueFacts>();
        }

        [Fact]
        public async Task ProcessesAllWorkAsync()
        {
            var expected = Enumerable.Range(0, 20).ToList();
            var concurrentBag = new ConcurrentBag<int>();

            var taskQueue = new TaskQueue<int>(
                workerCount: 8,
                produceAsync: (ctx, token) =>
                {
                    foreach (var i in expected)
                    {
                        ctx.Enqueue(i);
                    }
                    return Task.CompletedTask;
                },
                consumeAsync: (x, token) =>
                {
                    concurrentBag.Add(x);
                    return Task.CompletedTask;
                },
                logger: _logger);

            await taskQueue.RunAsync();

            Assert.Equal(expected, concurrentBag.OrderBy(x => x).ToList());
        }

        [Fact]
        public async Task AllowsNoWork()
        {
            var consumeCount = 0;
            var taskQueue = new TaskQueue<int>(
                workerCount: 8,
                produceAsync: (ctx, token) =>
                {
                    return Task.CompletedTask;
                },
                consumeAsync: (x, token) =>
                {
                    Interlocked.Increment(ref consumeCount);
                    return Task.CompletedTask;
                },
                logger: _logger);

            await taskQueue.RunAsync();

            Assert.Equal(0, consumeCount);
        }

        [Fact]
        public async Task ThrowsConsumerException()
        {
            InvalidOperationException expected = null;
            var consumeCount = 0;
            var taskQueue = new TaskQueue<int>(
                workerCount: 1,
                produceAsync: (ctx, token) =>
                {
                    Enqueue(ctx, 10);
                    return Task.CompletedTask;
                },
                consumeAsync: (x, token) =>
                {
                    if (Interlocked.Increment(ref consumeCount) >= 3)
                    {
                        expected = new InvalidOperationException("Fail!");
                        throw expected;
                    }

                    return Task.CompletedTask;
                },
                logger: _logger);

            var actual = await Assert.ThrowsAsync<InvalidOperationException>(
                () => taskQueue.RunAsync());
            Assert.Same(expected, actual);
            Assert.Equal(3, consumeCount);
        }

        [Fact]
        public async Task ConsumerExceptionCancelsOtherConsumers()
        {
            InvalidOperationException expected = null;
            var consumeCount = 0;
            var waitDuration = TimeSpan.FromSeconds(5);
            var failConsumerStarted = new TaskCompletionSource<bool>();
            var otherConsumerNotCanceled = false;

            var taskQueue = new TaskQueue<int>(
                workerCount: 2,
                produceAsync: (ctx, token) =>
                {
                    Enqueue(ctx, 10);
                    return Task.CompletedTask;
                },
                consumeAsync: async (x, token) =>
                {
                    var thisConsumeCount = Interlocked.Increment(ref consumeCount);
                    if (thisConsumeCount == 1)
                    {
                        await failConsumerStarted.Task;
                        await Task.Delay(waitDuration, token);
                        otherConsumerNotCanceled = true;
                    }
                    else if (thisConsumeCount == 2)
                    {
                        failConsumerStarted.TrySetResult(true);
                        expected = new InvalidOperationException("Fail!");
                        throw expected;
                    }
                },
                logger: _logger);

            var stopwatch = Stopwatch.StartNew();
            var actual = await Assert.ThrowsAsync<InvalidOperationException>(
                () => taskQueue.RunAsync());
            stopwatch.Stop();
            Assert.False(otherConsumerNotCanceled);
            Assert.Same(expected, actual);
            Assert.Equal(2, consumeCount);
            Assert.InRange(stopwatch.Elapsed, TimeSpan.Zero, waitDuration.Subtract(TimeSpan.FromTicks(1)));
        }

        private static void Enqueue(TaskQueue<int>.IProducerContext ctx, int count)
        {
            foreach (var i in Enumerable.Range(0, count))
            {
                ctx.Enqueue(i);
            }
        }
    }
}
