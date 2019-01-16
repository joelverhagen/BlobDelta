using System;
using System.Collections.Generic;
using System.Linq;
using Knapcode.Delta.Common;
using Microsoft.Azure.Search.Models;

namespace Knapcode.SearchDelta
{
    public class DocumentComparisonEnumerable : ComparisonEnumerable<DocumentContext, DocumentContext, DocumentComparison>
    {
        private static readonly IReadOnlyList<string> EmptyList = new List<string>();

        private readonly string _keyName;

        public DocumentComparisonEnumerable(
            IAsyncEnumerable<DocumentContext> left,
            IAsyncEnumerable<DocumentContext> right,
            string keyName) : base(left, right)
        {
            _keyName = keyName ?? throw new ArgumentNullException(nameof(keyName));
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

            var keyComparison = string.CompareOrdinal(GetKey(left.Document), GetKey(right.Document));
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
            var fieldsWithDifferentValues = InternEmpty(sharedFields.Where(x => !left.Document[x].Equals(right.Document[x])));

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

        private string GetKey(Document document)
        {
            return (string)document[_keyName];
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
            return new DocumentComparison(type, left, right, EmptyList, EmptyList, EmptyList);
        }
    }
}
