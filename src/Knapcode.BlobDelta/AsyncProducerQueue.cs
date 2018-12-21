using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Knapcode.BlobDelta
{
    public delegate Task<IEnumerable<T>> ProduceItemsAsync<T>(T item);

    public class AsyncProducerQueue<T> : IDisposable
    {
        private readonly ProduceItemsAsync<T> _produceItemsAsync;
        private readonly AsyncBlockingQueue<T> _queue;
        private int _inProgressCount;

        public AsyncProducerQueue(
            ProduceItemsAsync<T> produceItemsAsync,
            IEnumerable<T> initialItems)
        {
            _produceItemsAsync = produceItemsAsync ?? throw new ArgumentNullException(nameof(produceItemsAsync));
            _queue = new AsyncBlockingQueue<T>();
            _queue.EnqueueRange(initialItems);
        }

        public async Task ExecuteAsync()
        {
            await Task.Yield();

            while (true)
            {
                var result = await _queue.TryDequeueAsync();
                if (!result.HasItem)
                {
                    return;
                }

                Interlocked.Increment(ref _inProgressCount);

                var items = await _produceItemsAsync(result.Item);

                _queue.EnqueueRange(items);

                Interlocked.Decrement(ref _inProgressCount);

                if (_queue.Count == 0 && _inProgressCount == 0)
                {
                    _queue.MarkAsComplete();
                }
            }
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}
