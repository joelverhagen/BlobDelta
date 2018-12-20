using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class ComparableBlob : IComparableBlob
    {
        private ComparableBlob(ICloudBlob blob)
        {
            Blob = blob ?? throw new ArgumentNullException(nameof(blob));
        }

        protected ICloudBlob Blob { get; }

        public virtual string Name => Blob.Name;
        public virtual Type BlobType => Blob.GetType();
        public virtual long Length => Blob.Properties.Length;
        public virtual string ContentMD5 => Blob.Properties.ContentMD5;

        public static ComparableBlob CreateOrNull(BlobContext blobContext)
        {
            if (blobContext == null)
            {
                return null;
            }

            return new ComparableBlob(blobContext.Blob);
        }
    }
}
