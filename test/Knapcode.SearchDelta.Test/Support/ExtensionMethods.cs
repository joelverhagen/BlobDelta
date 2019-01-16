using System.Collections.Generic;
using System.Linq;

namespace Knapcode.SearchDelta.Test.Support
{
    public static class ExtensionMethods
    {
        public static string[] GetField(this IEnumerable<DocumentContext> output, string field)
        {
            return output.Select(x => (string)x.Document[field]).ToArray();
        }
    }
}
