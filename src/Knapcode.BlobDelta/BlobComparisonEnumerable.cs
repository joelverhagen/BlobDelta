using System;
using Knapcode.Delta.Common;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Knapcode.BlobDelta
{
    public class BlobComparisonEnumerable : ComparisonEnumerable<BlobContext, BlobContext, BlobComparison>
    {
        private readonly IAsyncEnumerable<BlobContext> _left;
        private readonly IAsyncEnumerable<BlobContext> _right;

        public BlobComparisonEnumerable(
            IAsyncEnumerable<BlobContext> left,
            IAsyncEnumerable<BlobContext> right) : base(left, right)
        {
            _left = left ?? throw new ArgumentNullException(nameof(left));
            _right = right ?? throw new ArgumentNullException(nameof(right));
        }

        protected override BlobComparison Compare(BlobContext left, BlobContext right)
        {
            var leftComparable = GetLeftComparableBlobOrNull(left);
            var rightComparable = GetRightComparableBlobOrNull(right);
            var type = Compare(leftComparable, rightComparable);

            if (type == BlobComparisonType.MissingFromLeft)
            {
                left = null;
            }
            else if (type == BlobComparisonType.MissingFromRight)
            {
                right = null;
            }

            return new BlobComparison(type, left, right);
        }

        protected virtual IComparableBlob GetLeftComparableBlobOrNull(BlobContext left)
        {
            return ComparableBlob.CreateOrNull(left);
        }

        protected virtual IComparableBlob GetRightComparableBlobOrNull(BlobContext right)
        {
            return ComparableBlob.CreateOrNull(right);
        }

        protected virtual BlobComparisonType Compare(IComparableBlob left, IComparableBlob right)
        {
            if (left == null)
            {
                if (right == null)
                {
                    throw new InvalidOperationException("Only one side should be null.");
                }

                return BlobComparisonType.MissingFromLeft;
            }

            if (right == null)
            {
                return BlobComparisonType.MissingFromRight;
            }

            var nameComparison = string.CompareOrdinal(left.Name, right.Name);
            if (nameComparison < 0)
            {
                return BlobComparisonType.MissingFromRight;
            }
            else if (nameComparison > 0)
            {
                return BlobComparisonType.MissingFromLeft;
            }

            var leftType = left.BlobType;
            if (leftType != right.BlobType)
            {
                return BlobComparisonType.DifferentBlobType;
            }

            if (leftType != typeof(CloudBlockBlob))
            {
                return BlobComparisonType.UnsupportedBlobType;
            }

            if (left.Length != right.Length)
            {
                return BlobComparisonType.DifferentContent;
            }

            if (left.ContentMD5 == null || right.ContentMD5 == null)
            {
                return BlobComparisonType.MissingContentMD5;
            }

            if (left.ContentMD5 != right.ContentMD5)
            {
                return BlobComparisonType.DifferentContent;
            }

            return BlobComparisonType.Same;
        }
    }
}
