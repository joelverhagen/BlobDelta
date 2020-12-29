using Microsoft.WindowsAzure.Storage.Blob;
using System;

namespace Knapcode.BlobDelta
{
    public class BlobContext
    {
        public BlobContext(
            ICloudBlob blob,
            BlobContinuationToken continuationToken,
            int segmentIndex,
            int blobIndex)
        {
            Blob = blob ?? throw new ArgumentNullException(nameof(blob));
            ContinuationToken = continuationToken;
            SegmentIndex = segmentIndex;
            BlobIndex = blobIndex;
        }

        public ICloudBlob Blob { get; }
        public BlobContinuationToken ContinuationToken { get; }
        public int SegmentIndex { get; }
        public int BlobIndex { get; }
    }
}
