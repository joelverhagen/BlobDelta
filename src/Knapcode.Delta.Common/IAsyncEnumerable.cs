namespace Knapcode.Delta.Common
{
    public interface IAsyncEnumerable<T>
    {
        IAsyncEnumerator<T> GetEnumerator();
    }
}
