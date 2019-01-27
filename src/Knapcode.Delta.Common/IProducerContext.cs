using System.Threading;
using System.Threading.Tasks;

namespace Knapcode.Delta.Common
{
    public interface IProducerContext<T>
    {
        int Count { get; }
        Task EnqueueAsync(T item, CancellationToken token);
    }
}
