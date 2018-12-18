using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class BlobAndContinuationToken
    {
        public BlobAndContinuationToken(ICloudBlob blob, BlobContinuationToken continuationToken)
        {
            Blob = blob ?? throw new ArgumentNullException(nameof(blob));
            ContinuationToken = continuationToken;
        }

        public ICloudBlob Blob { get; }
        public BlobContinuationToken ContinuationToken { get; }
    }
}
