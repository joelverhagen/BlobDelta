using System;
using System.Collections.Generic;
using System.Linq;

namespace Knapcode.BlobDelta
{
    public class BlobComparison : IComparison
    {
        private static ISet<BlobComparisonType> ValidTypes = new HashSet<BlobComparisonType>(Enum
            .GetValues(typeof(BlobComparisonType))
            .Cast<BlobComparisonType>());

        public BlobComparison(
            BlobComparisonType type,
            BlobAndContinuationToken left,
            BlobAndContinuationToken right)
        {
            if (!ValidTypes.Contains(type))
            {
                throw new ArgumentException("The provided blob comparison type is not valid.", nameof(type));
            }

            if (left == null && right == null)
            {
                throw new ArgumentException($"Either {nameof(left)} or {nameof(right)} (or both) must be non-null.");
            }

            Type = type;
            Left = left;
            Right = right;
        }

        public BlobComparisonType Type { get; }
        public BlobAndContinuationToken Left { get; }
        public BlobAndContinuationToken Right { get; }
        public bool IsMissingFromLeft => Type == BlobComparisonType.MissingFromLeft;
        public bool IsMissingFromRight => Type == BlobComparisonType.MissingFromRight;
    }
}
