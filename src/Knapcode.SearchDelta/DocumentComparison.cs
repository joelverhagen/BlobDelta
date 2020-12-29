using Knapcode.Delta.Common;
using System.Collections.Generic;

namespace Knapcode.SearchDelta
{
    public class DocumentComparison : IComparison
    {
        public DocumentComparison(
            DocumentComparisonType type,
            DocumentContext left,
            DocumentContext right,
            IReadOnlyList<string> fieldsMissingFromLeft,
            IReadOnlyList<string> fieldsMissingFromRight,
            IReadOnlyList<string> fieldsWithDifferentValues)
        {
            Type = type;
            Left = left;
            Right = right;
            FieldsMissingFromLeft = fieldsMissingFromLeft;
            FieldsMissingFromRight = fieldsMissingFromRight;
            FieldsWithDifferentValues = fieldsWithDifferentValues;
        }

        public DocumentComparisonType Type { get; }
        public DocumentContext Left { get; }
        public DocumentContext Right { get; }
        public IReadOnlyList<string> FieldsMissingFromLeft { get; }
        public IReadOnlyList<string> FieldsMissingFromRight { get; }
        public IReadOnlyList<string> FieldsWithDifferentValues { get; }

        public bool IsMissingFromLeft => Type == DocumentComparisonType.MissingFromLeft;
        public bool IsMissingFromRight => Type == DocumentComparisonType.MissingFromRight;
    }
}
