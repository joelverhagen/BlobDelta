using System;
using System.Threading;
using System.Threading.Tasks;

namespace Knapcode.BlobDelta
{
    internal class DisposableSemaphoreSlim : IDisposable
    {
        private const int FalseInt = 0;
        private const int TrueInt = 1;
        private readonly SemaphoreSlim _semaphoreSlim;
        private int _disposed;

        private DisposableSemaphoreSlim(SemaphoreSlim semaphoreSlim)
        {
            _semaphoreSlim = semaphoreSlim ?? throw new ArgumentNullException(nameof(semaphoreSlim));
            _disposed = FalseInt;
        }

        public static async Task<DisposableSemaphoreSlim> WaitAsync(SemaphoreSlim semaphoreSlim)
        {
            bool acquired = false;
            try
            {
                await semaphoreSlim.WaitAsync();
                acquired = true;
                return new DisposableSemaphoreSlim(semaphoreSlim);
            }
            finally
            {
                if (acquired)
                {
                    semaphoreSlim.Release();
                }
            }
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref _disposed, TrueInt, FalseInt) == FalseInt)
            {
                _semaphoreSlim.Release();
            }
        }
    }
}
