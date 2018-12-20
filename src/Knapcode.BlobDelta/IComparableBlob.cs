using System;

namespace Knapcode.BlobDelta
{
    public interface IComparableBlob
    {
        string Name { get; }
        Type BlobType { get; }
        long Length { get; }
        string ContentMD5 { get; }
    }
}
