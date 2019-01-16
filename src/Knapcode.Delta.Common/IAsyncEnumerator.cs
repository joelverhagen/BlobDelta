using System.Threading.Tasks;

namespace Knapcode.Delta.Common
{
    public interface IAsyncEnumerator<T>
    {
        T Current { get; }
        Task<bool> MoveNextAsync();
    }
}
