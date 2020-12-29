using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Knapcode.Delta.Common
{
    public class TaskQueue<T>
    {
        private readonly int _workerCount;
        private readonly int _maxQueueSize;
        private readonly ProduceAsync _produceAsync;
        private readonly ConsumeAsync _consumeAsync;
        private readonly ILogger _logger;

        public TaskQueue(
            int workerCount,
            int maxQueueSize,
            ProduceAsync produceAsync,
            ConsumeAsync consumeAsync,
            ILogger logger)
        {
            _workerCount = workerCount;
            _maxQueueSize = maxQueueSize;
            _produceAsync = produceAsync;
            _consumeAsync = consumeAsync;
            _logger = logger;
        }

        public TaskQueue(
            int workerCount,
            ProduceAsync produceAsync,
            ConsumeAsync consumeAsync,
            ILogger logger)
        {
            _workerCount = workerCount;
            _maxQueueSize = DataflowBlockOptions.Unbounded;
            _produceAsync = produceAsync;
            _consumeAsync = consumeAsync;
            _logger = logger;
        }

        public async Task RunAsync()
        {
            using (var execution = new TaskQueueExecution(
               _workerCount,
               _maxQueueSize,
               _produceAsync,
               _consumeAsync,
               _logger))
            {
                await execution.RunAsync().ConfigureAwait(false);
            }
        }

        public delegate Task ProduceAsync(IProducerContext<T> context, CancellationToken token);
        public delegate Task ConsumeAsync(T item, CancellationToken token);

        private class TaskQueueExecution : IDisposable
        {
            private readonly int _workerCount;
            private readonly int _maxQueueSize;
            private readonly ProduceAsync _produceAsync;
            private readonly ConsumeAsync _consumeAsync;
            private readonly ILogger _logger;
            private readonly CancellationTokenSource _failureCts;
            private readonly BufferBlock<T> _producer;
            private readonly ActionBlock<T> _consumer;
            private readonly IDisposable _disposable;

            public TaskQueueExecution(
                int workerCount,
                int maxQueueSize,
                ProduceAsync produceAsync,
                ConsumeAsync consumeAsync,
                ILogger logger)
            {
                _workerCount = workerCount;
                _maxQueueSize = maxQueueSize;
                _produceAsync = produceAsync;
                _consumeAsync = consumeAsync;
                _logger = logger;
                _failureCts = new CancellationTokenSource();

                _producer = new BufferBlock<T>(new DataflowBlockOptions
                {
                    CancellationToken = _failureCts.Token,
                    BoundedCapacity = _maxQueueSize,
                });

                _consumer = new ActionBlock<T>(
                    x => ConsumeAsync(x),
                    new ExecutionDataflowBlockOptions
                    {
                        CancellationToken = _failureCts.Token,
                        MaxDegreeOfParallelism = workerCount,
                    });

                _disposable = _producer.LinkTo(
                    _consumer,
                    new DataflowLinkOptions
                    {
                        PropagateCompletion = true,
                    });
            }

            private async Task ConsumeAsync(T x)
            {
                try
                {
                    await _consumeAsync(x, _failureCts.Token).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(0, ex, "A consumer in the task queue encountered an exception.");
                    _failureCts.Cancel();
                    throw;
                }
            }

            public void Dispose()
            {
                _disposable.Dispose();
                _failureCts.Dispose();
            }

            public async Task RunAsync()
            {
                var producerContext = new ProducerContext(_producer);

                await Task.WhenAll(
                    ProduceThenCompleteAsync(producerContext),
                    _producer.Completion,
                    _consumer.Completion).ConfigureAwait(false);
            }

            private async Task ProduceThenCompleteAsync(ProducerContext producerContext)
            {
                await Task.Yield();
                try
                {
                    await _produceAsync(producerContext, _failureCts.Token).ConfigureAwait(false);
                    _producer.Complete();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(0, ex, "A producer in the task queue encountered an exception.");
                    _failureCts.Cancel();
                    throw;
                }
            }

            private class ProducerContext : IProducerContext<T>
            {
                private readonly BufferBlock<T> _queue;

                public ProducerContext(BufferBlock<T> queue)
                {
                    _queue = queue;
                }

                public int Count => _queue.Count;

                public async Task EnqueueAsync(T item, CancellationToken token)
                {
                    await _queue.SendAsync(item, token).ConfigureAwait(false);
                }
            }
        }
    }
}
