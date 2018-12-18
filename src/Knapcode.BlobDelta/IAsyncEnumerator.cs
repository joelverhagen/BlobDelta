using System.Threading.Tasks;

namespace Knapcode.BlobDelta
{
    public interface IAsyncEnumerator<T>
    {
        T Current { get; }
        Task<bool> MoveNextAsync();
    }
}
