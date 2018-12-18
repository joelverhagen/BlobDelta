namespace Knapcode.BlobDelta
{
    public enum BlobComparisonType
    {
        Same,
        MissingFromLeft,
        MissingFromRight,
        DifferentBlobType,
        UnsupportedBlobType,
        DifferentContent,
        MissingContentMD5,
    }
}
