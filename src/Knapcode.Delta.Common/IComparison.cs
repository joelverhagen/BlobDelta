namespace Knapcode.Delta.Common
{
    public interface IComparison
    {
        bool IsMissingFromLeft { get; }
        bool IsMissingFromRight { get; }
    }
}
