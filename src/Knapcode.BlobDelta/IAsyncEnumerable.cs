namespace Knapcode.BlobDelta
{
    public interface IAsyncEnumerable<T>
    {
        IAsyncEnumerator<T> GetEnumerator();
    }
}
