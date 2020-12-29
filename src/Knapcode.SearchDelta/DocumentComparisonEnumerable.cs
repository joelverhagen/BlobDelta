using Knapcode.Delta.Common;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Knapcode.SearchDelta
{
    public class DocumentComparisonEnumerable : ComparisonEnumerable<DocumentContext, DocumentContext, DocumentComparison>
    {
        private static readonly IReadOnlyList<string> EmptyList = new List<string>();

        public DocumentComparisonEnumerable(
            IAsyncEnumerable<DocumentContext> left,
            IAsyncEnumerable<DocumentContext> right) : base(left, right)
        {
        }

        protected override DocumentComparison Compare(DocumentContext left, DocumentContext right)
        {
            if (left == null)
            {
                if (right == null)
                {
                    throw new InvalidOperationException("Only one side should be null.");
                }

                return EmptyLists(DocumentComparisonType.MissingFromLeft, left, right);
            }

            if (right == null)
            {
                return EmptyLists(DocumentComparisonType.MissingFromRight, left, right);
            }

            var keyComparison = string.CompareOrdinal(left.Key, right.Key);
            if (keyComparison < 0)
            {
                return EmptyLists(DocumentComparisonType.MissingFromRight, left, right);
            }
            else if (keyComparison > 0)
            {
                return EmptyLists(DocumentComparisonType.MissingFromLeft, left, right);
            }

            var fieldsMissingFromRight = InternEmpty(left.Document.Keys.Except(right.Document.Keys));
            var fieldsMissingFromLeft = InternEmpty(right.Document.Keys.Except(left.Document.Keys));
            var sharedFields = InternEmpty(left.Document.Keys.Intersect(right.Document.Keys));

            var fieldsWithDifferentValues = InternEmpty(sharedFields.Where(x => !FieldEquals(left, right, x)));

            if (fieldsMissingFromRight.Any()
                || fieldsMissingFromLeft.Any()
                || fieldsWithDifferentValues.Any())
            {
                return new DocumentComparison(
                    DocumentComparisonType.DifferentFields,
                    left,
                    right,
                    fieldsMissingFromLeft,
                    fieldsMissingFromRight,
                    fieldsWithDifferentValues);
            }
            else
            {
                return EmptyLists(DocumentComparisonType.Same, left, right);
            }
        }

        private static bool FieldEquals(DocumentContext left, DocumentContext right, string name)
        {
            var leftField = left.Document[name];
            var rightField = right.Document[name];

            var leftSequence = leftField as System.Collections.IEnumerable;
            var rightSequence = rightField as System.Collections.IEnumerable;
            if (leftSequence != null && rightSequence != null)
            {
                return leftSequence.Cast<object>().SequenceEqual(rightSequence.Cast<object>());
            }
            else
            {
                return Equals(left.Document[name], right.Document[name]);
            }
        }

        private static IReadOnlyList<string> InternEmpty(IEnumerable<string> input)
        {
            var list = input.ToList();
            if (list.Any())
            {
                return list;
            }
            else
            {
                return EmptyList;
            }
        }

        private static DocumentComparison EmptyLists(
            DocumentComparisonType type,
            DocumentContext left,
            DocumentContext right)
        {
            if (type == DocumentComparisonType.MissingFromLeft)
            {
                left = null;
            }

            if (type == DocumentComparisonType.MissingFromRight)
            {
                right = null;
            }

            return new DocumentComparison(type, left, right, EmptyList, EmptyList, EmptyList);
        }
    }
}
