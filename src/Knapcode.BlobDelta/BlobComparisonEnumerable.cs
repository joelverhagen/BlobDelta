using System;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class BlobComparisonEnumerable : ComparisonEnumerable<BlobAndContinuationToken, BlobAndContinuationToken, BlobComparison>
    {
        private readonly IAsyncEnumerable<BlobAndContinuationToken> _left;
        private readonly IAsyncEnumerable<BlobAndContinuationToken> _right;

        public BlobComparisonEnumerable(
            IAsyncEnumerable<BlobAndContinuationToken> left,
            IAsyncEnumerable<BlobAndContinuationToken> right) : base(left, right, CompareBlobs)
        {
            _left = left ?? throw new ArgumentNullException(nameof(left));
            _right = right ?? throw new ArgumentNullException(nameof(right));
        }

        private static BlobComparison CompareBlobs(BlobAndContinuationToken left, BlobAndContinuationToken right)
        {
            if (left == null)
            {
                if (right == null)
                {
                    throw new InvalidOperationException("Only one side should be null.");
                }

                return new BlobComparison(BlobComparisonType.MissingFromLeft, left, right);
            }

            if (right == null)
            {
                return new BlobComparison(BlobComparisonType.MissingFromRight, left, right);
            }

            var nameComparison = string.CompareOrdinal(left.Blob.Name, right.Blob.Name);
            if (nameComparison < 0)
            {
                return new BlobComparison(BlobComparisonType.MissingFromRight, left: left, right: null);
            }
            else if (nameComparison > 0)
            {
                return new BlobComparison(BlobComparisonType.MissingFromLeft, left: null, right: right);
            }

            var leftType = left.Blob.GetType();
            if (leftType != right.Blob.GetType())
            {
                return new BlobComparison(BlobComparisonType.DifferentBlobType, left, right);
            }

            if (leftType != typeof(CloudBlockBlob))
            {
                return new BlobComparison(BlobComparisonType.UnsupportedBlobType, left, right);
            }

            var leftBlob = (CloudBlockBlob)left.Blob;
            var rightBlob = (CloudBlockBlob)right.Blob;

            if (leftBlob.Properties.Length != rightBlob.Properties.Length)
            {
                return new BlobComparison(BlobComparisonType.DifferentContent, left, right);
            }

            if (leftBlob.Properties.ContentMD5 == null || rightBlob.Properties.ContentMD5 == null)
            {
                return new BlobComparison(BlobComparisonType.MissingContentMD5, left, right);
            }

            if (leftBlob.Properties.ContentMD5 != rightBlob.Properties.ContentMD5)
            {
                return new BlobComparison(BlobComparisonType.DifferentContent, left, right);
            }

            return new BlobComparison(BlobComparisonType.Same, left, right);
        }
    }
}
