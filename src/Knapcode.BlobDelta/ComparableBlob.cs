using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class ComparableBlob : IComparableBlob
    {
        private readonly ICloudBlob _blob;

        private ComparableBlob(ICloudBlob blob)
        {
            _blob = blob ?? throw new ArgumentNullException(nameof(blob));
        }

        public string Name => _blob.Name;
        public Type BlobType => _blob.GetType();
        public long Length => _blob.Properties.Length;
        public string ContentMD5 => _blob.Properties.ContentMD5;

        public static ComparableBlob CreateOrNull(BlobAndContinuationToken blobAndContinuationToken)
        {
            if (blobAndContinuationToken == null)
            {
                return null;
            }

            return new ComparableBlob(blobAndContinuationToken.Blob);
        }
    }
}
