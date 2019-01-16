using System.Collections.Generic;
using System.Linq;

namespace Knapcode.BlobDelta.Test.Support
{
    public static class ExtensionMethods
    {
        public static string[] GetBlobNames(this IEnumerable<BlobContext> output)
        {
            return output.Select(x => x.Blob.Name).ToArray();
        }
    }
}
