using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Knapcode.Delta.Common
{
    public class TaskQueue<T>
    {
        private readonly int _workerCount;
        private readonly ProduceAsync _produceAsync;
        private readonly ConsumeAsync _consumeAsync;
        private readonly ILogger _logger;

        public TaskQueue(
            int workerCount,
            ProduceAsync produceAsync,
            ConsumeAsync consumeAsync,
            ILogger logger)
        {
            _workerCount = workerCount;
            _produceAsync = produceAsync;
            _consumeAsync = consumeAsync;
            _logger = logger;
        }

        public async Task RunAsync()
        {
            using (var execution = new TaskQueueExecution(
                _workerCount,
                _produceAsync,
                _consumeAsync,
                _logger))
            {
                await execution.RunAsync().ConfigureAwait(false);
            }
        }

        public delegate Task ProduceAsync(IProducerContext context, CancellationToken token);
        public delegate Task ConsumeAsync(T item, CancellationToken token);

        public interface IProducerContext
        {
            void Enqueue(T item);
            Task WaitForCountToBeLessThanAsync(int lessThan);
        }

        private class TaskQueueExecution : IDisposable
        {
            private readonly int _workerCount;
            private readonly ProduceAsync _produceAsync;
            private readonly ConsumeAsync _consumeAsync;
            private readonly ILogger _logger;

            private readonly AsyncBlockingQueue<T> _queue;
            private readonly CancellationTokenSource _failureCts;
            private readonly TaskCompletionSource<Task> _failureTcs;
            private IReadOnlyList<Task> _consumers;

            public TaskQueueExecution(
                int workerCount,
                ProduceAsync produceAsync,
                ConsumeAsync consumeAsync,
                ILogger logger)
            {
                _workerCount = workerCount;
                _produceAsync = produceAsync;
                _consumeAsync = consumeAsync;
                _logger = logger;

                _queue = new AsyncBlockingQueue<T>();
                _failureCts = new CancellationTokenSource();
                _failureTcs = new TaskCompletionSource<Task>();
            }

            public async Task RunAsync()
            {
                // Start the consumers.
                _consumers = Enumerable
                    .Range(0, _workerCount)
                    .Select(x => ConsumeUntilCompleteAsync())
                    .ToList();

                // Start the producer.
                var produceThenCompleteTask = ProduceThenCompleteAsync();

                // Wait for completion or failure, whichever happens first.
                var failureTask = _failureTcs.Task;
                var firstTask = await Task.WhenAny(failureTask, produceThenCompleteTask).ConfigureAwait(false);
                if (firstTask == failureTask)
                {
                    await await failureTask.ConfigureAwait(false);
                }
                else
                {
                    await produceThenCompleteTask.ConfigureAwait(false);
                }
            }

            public void Dispose()
            {
                _queue.Dispose();
                _failureCts.Dispose();
            }

            private async Task WaitForCountToBeLessThanAsync(int lessThan)
            {
                var logged = false;
                while (_queue.Count >= lessThan)
                {
                    if (!logged)
                    {
                        _logger.LogInformation(
                            "There are {Count} units of work in the queue. Waiting till the queue size decreases below {LessThan}.",
                            _queue.Count,
                            lessThan);
                        logged = true;
                    }

                    await Task.Delay(TimeSpan.FromMilliseconds(100)).ConfigureAwait(false);
                }

                if (logged)
                {
                    _logger.LogInformation(
                        "There are now {Count} batches of packages to be persisted. Proceeding with enqueueing.",
                        _queue.Count);
                }
            }

            private async Task ProduceThenCompleteAsync()
            {
                var produceTask = ProduceAsync();
                try
                {
                    await produceTask.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(0, ex, "The producer in the task queue encountered an exception.");
                    SetFailedTask(produceTask);
                    throw;
                }

                _queue.MarkAsComplete();
                await Task.WhenAll(_consumers).ConfigureAwait(false);
            }

            private async Task ProduceAsync()
            {
                await Task.Yield();
                await _produceAsync(new ProducerContext(this), _failureCts.Token).ConfigureAwait(false);
            }

            private void Enqueue(T item)
            {
                _queue.Enqueue(item);
            }

            private async Task ConsumeUntilCompleteAsync()
            {
                await Task.Yield();
                bool hasItem;
                do
                {
                    var result = await _queue.TryDequeueAsync().ConfigureAwait(false);
                    hasItem = result.HasItem;

                    if (hasItem)
                    {
                        var consumeTask = ConsumeAsync(result);
                        try
                        {
                            await consumeTask.ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning(0, ex, "A worker in the task queue encountered an exception.");
                            SetFailedTask(consumeTask);
                            throw;
                        }
                    }
                }
                while (!_failureCts.IsCancellationRequested && hasItem);
            }

            private async Task ConsumeAsync(DequeueResult<T> result)
            {
                await Task.Yield();
                await _consumeAsync(result.Item, _failureCts.Token).ConfigureAwait(false);
            }

            private void SetFailedTask(Task task)
            {
                _failureTcs.TrySetResult(task);
                _failureCts.Cancel();
            }

            private class ProducerContext : IProducerContext
            {
                private readonly TaskQueueExecution _execution;

                public ProducerContext(TaskQueueExecution execution)
                {
                    _execution = execution;
                }

                public void Enqueue(T item)
                {
                    _execution.Enqueue(item);
                }

                public async Task WaitForCountToBeLessThanAsync(int lessThan)
                {
                    await _execution.WaitForCountToBeLessThanAsync(lessThan).ConfigureAwait(false);
                }
            }
        }
    }
}
