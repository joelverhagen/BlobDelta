namespace Knapcode.BlobDelta
{
    public interface IComparison
    {
        bool IsMissingFromLeft { get; }
        bool IsMissingFromRight { get; }
    }
}
